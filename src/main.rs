/*
 * Copyright 2023 Oxide Computer Company
 */

use std::process::{Command, Stdio, exit};
use anyhow::{Result, Context, bail, anyhow};
use std::collections::HashMap;
use jmclib::log::prelude::*;
use serde::Deserialize;
use std::path::{PathBuf, Path};
use uuid::Uuid;
use std::io::{Read, Write};

mod lofi;
mod ensure;
mod illumos;
mod expand;
mod fmri;

use ensure::{Create, HashType};
use expand::Expansion;

type Build = fn(ib: &mut ImageBuilder) -> Result<()>;

/*
 * Hard-coded user ID and group ID for root:
 */
const ROOT: u32 = 0;

/*
 * We cannot correctly use the name service switch to translate user IDs for use
 * in the target image, as the database within the target may not match the
 * build system.  For now, assume we only need to deal with a handful of
 * hard-coded user names.
 */
fn translate_uid(user: &str) -> Result<u32> {
    Ok(match user {
        "root" => ROOT,
        "daemon" => 1,
        "bin" => 2,
        "sys" => 3,
        "adm" => 4,
        n => bail!("unknown user \"{}\"", n),
    })
}

/*
 * The situation is the same for group IDs as it is for user IDs.  See comments
 * for translate_uid().
 */
fn translate_gid(group: &str) -> Result<u32> {
    Ok(match group {
        "root" => ROOT,
        "other" => 1,
        "bin" => 2,
        "sys" => 3,
        "adm" => 4,
        n => bail!("unknown group \"{}\"", n),
    })
}

fn main() -> Result<()> {
    let cmd = std::env::args().nth(1)
        .ok_or_else(||anyhow!("missing command name"))?;

    let mut opts = getopts::Options::new();
    opts.parsing_style(getopts::ParsingStyle::StopAtFirstFree);

    fn usage(opts: &getopts::Options) {
        let s = opts.usage("image-builder");
        println!("{}", s);
    }

    let f = match cmd.as_str() {
        "build" => {
            opts.optflag("r", "reset", "destroy any work-in-progress dataset");
            opts.optflag("x", "fullreset", "destroy dataset");
            opts.reqopt("g", "group", "group name", "GROUPNAME");
            opts.reqopt("n", "name", "image name", "IMAGENAME");
            opts.optopt("N", "output-name", "output image name", "IMAGENAME");
            opts.reqopt("d", "dataset", "root dataset for work", "DATASET");
            opts.optopt("T", "templates", "directory for templates", "DIR");
            opts.optopt("S", "svccfg", "svccfg-native location", "SVCCFG");
            opts.optmulti("F", "feature", "add or remove a feature definition",
                "[^]FEATURE[=VALUE]");
            opts.optmulti("E", "external-src", "external file src tree",
                "DIR");

            run_build
        }
        n => {
            usage(&opts);
            bail!("invalid command: {}", n);
        }
    };

    let log = jmclib::log::init_log();
    let mat = match opts.parse(std::env::args().skip(2)) {
        Ok(mat) => mat,
        Err(e) => {
            usage(&opts);
            bail!("invalid options: {:?}", e);
        }
    };

    if let Err(e) = f(&log, &mat) {
        crit!(log, "fatal error: {:?}", e);
        exit(1);
    }

    Ok(())
}

/**
 * If a lofi device for this file exists, detach it.  Returns true if a lofi
 * device was found, otherwise returns false.
 */
fn teardown_lofi<P: AsRef<Path>>(log: &Logger, imagefile: P) -> Result<bool> {
    let imagefile = imagefile.as_ref();

    let lofis = lofi::lofi_list()?;
    let matches: Vec<_> = lofis.iter()
        .filter(|li| li.filename == imagefile)
        .collect();
    if !matches.is_empty() {
        if matches.len() != 1 {
            bail!("too many lofis");
        }

        let li = &matches[0];

        info!(log, "lofi exists {:?} -- removing...", li);
        lofi::lofi_unmap_device(&li.devpath.as_ref().unwrap())?;
        Ok(true)
    } else {
        info!(log, "no lofi found");
        Ok(false)
    }
}

fn attach_lofi<P: AsRef<Path>>(log: &Logger, imagefile: P, label: bool)
    -> Result<lofi::LofiDevice>
{
    let lofi = lofi::lofi_map(&imagefile, label)?;
    info!(log, "lofi device = {}", lofi.devpath.as_ref().unwrap().display());

    Ok(lofi)
}

/**
 * Create a blank image file of size megabytes and attach it as a lofi(7D)
 * device.  If a lofi device for that file already exists, detach it first.  If
 * the image file already exists, remove it and replace it.  If label is true,
 * create a labelled lofi device (with partitions and slices), otherwise create
 * an unlabelled (simple) device.
 */
fn recreate_lofi<P: AsRef<Path>>(log: &Logger, imagefile: P, size: usize,
    label: bool)
    -> Result<lofi::LofiDevice>
{
    let imagefile = imagefile.as_ref();

    /*
     * Check to make sure it is not already in lofi:
     */
    teardown_lofi(log, imagefile)?;

    ensure::removed(log, imagefile)?;

    /*
     * Create the file that we will use as the backing store for the pool.  The
     * size of this file is the resultant disk image size.
     */
    mkfile(log, &imagefile, size)?;

    /*
     * Attach this file as a lofi(7D) device.
     */
    attach_lofi(log, imagefile, label)
}

/**
 * Offset and length (within an ISO image) of an El Torito boot image.  Sizes
 * are in bytes.
 */
#[derive(Debug)]
struct ElToritoEntry {
    offset: usize,
    length: usize,
}

/**
 * Add an MBR partition to the specified raw device.  Valid values for
 * "id" are described in "/usr/include/sys/dktp/fdisk.h"; of particular
 * note:
 *      X86BOOT     190     x86 illumos boot partition
 *      EFI_FS      239     EFI File System (System Partition)
 *
 * Both "start" and "nsectors" are specified as a count of 512 byte disk blocks.
 */
fn fdisk_add<P: AsRef<Path>>(log: &Logger, rdev: P, id: u8, start: u32,
    nsectors: u32) -> Result<()>
{
    let rdev = rdev.as_ref();

    ensure::run(log, &["/usr/sbin/fdisk",
        "-A", &format!("{}:0:0:0:0:0:0:0:{}:{}", id, start, nsectors),
        rdev.to_str().unwrap(),
    ])?;

    Ok(())
}

fn installboot<P1, P2, P3>(log: &Logger, rdev: P1, stage1: P2, stage2: P3)
    -> Result<()>
    where P1: AsRef<Path>, P2: AsRef<Path>, P3: AsRef<Path>,
{
    let rdev = rdev.as_ref();
    let stage1 = stage1.as_ref();
    let stage2 = stage2.as_ref();

    ensure::run(log, &["/usr/sbin/installboot",
        "-fm",
        stage1.to_str().unwrap(),
        stage2.to_str().unwrap(),
        rdev.to_str().unwrap(),
    ])?;

    Ok(())
}

fn etdump<P: AsRef<Path>>(log: &Logger, imagefile: P, platform: &str,
    system: &str) -> Result<ElToritoEntry>
{
    let imagefile = imagefile.as_ref();

    info!(log, "examining El Torito entries in {:?}", imagefile);

    let etdump = Command::new("/usr/bin/etdump")
        .env_clear()
        .arg("--format")
        .arg("shell")
        .arg(imagefile)
        .output()?;

    if !etdump.status.success() {
        let errmsg = String::from_utf8_lossy(&etdump.stderr);
        bail!("etdump failed: {}", errmsg);
    }

    for l in String::from_utf8(etdump.stdout)?.lines() {
        let mut m: HashMap<String, String> = HashMap::new();
        for t in l.split(';') {
            let kv = t.split('=').collect::<Vec<_>>();
            if kv.len() != 2 {
                bail!("unexpected term in etdump line: {:?}", l);
            }
            if let Some(k) = kv[0].strip_prefix("et_") {
                m.insert(k.to_string(), kv[1].to_string());
            }
        }

        if m.get("platform") == Some(&platform.to_string()) &&
            m.get("system") == Some(&system.to_string())
        {
            let offset = if let Some(lba) = m.get("lba") {
                lba.parse::<usize>()? * 2048
            } else {
                bail!("missing LBA?");
            };
            let length = if let Some(sectors) = m.get("sectors") {
                sectors.parse::<usize>()? * 512
            } else {
                bail!("missing sectors?");
            };

            return Ok(ElToritoEntry { offset, length, });
        }
    }

    bail!("could not find El Torito entry for (platform {} system {})",
        platform, system);
}

fn run_build_iso(ib: &mut ImageBuilder) -> Result<()> {
    let log = &ib.log;

    /*
     * Steps like "ensure_file" will have their root directory relative to
     * "/proto" in the temporary dataset.  This directory represents the root of
     * the target ISO image.
     */
    let rmp = ib.root()?;
    assert!(!rmp.exists());
    ensure::directory(log, &rmp, ROOT, ROOT, 0o755)?;

    /*
     * Now that we have created the proto directory, run the steps for this
     * template.
     */
    run_steps(ib)?;

    info!(ib.log, "steps complete; finalising image!");

    let iso = ib.template.iso.as_ref().unwrap();

    if iso.hybrid.is_some() && iso.boot_uefi.is_none() {
        /*
         * XXX Due to limitations in installboot(1M), it is not presently
         * possible to specify a device path for boot loader installation unless
         * there is a numbered partition (not the whole disk) on which an
         * identifiable file system is present.  Without the ESP image embedded
         * in the ISO, we have no such file system to specify.
         */
        bail!("presently all hybrid images must include UEFI support");
    }

    let imagefile = ib.work_file("output.iso")?;
    teardown_lofi(&ib.log, &imagefile)?;
    ensure::removed(&ib.log, &imagefile)?;

    let mut args = vec!["/usr/bin/mkisofs", "-N", "-l", "-R", "-U", "-d",
        "-D", "-c", ".catalog", "-allow-multidot", "-no-iso-translate",
        "-cache-inodes"];
    if let Some(volume_id) = iso.volume_id.as_deref() {
        args.push("-V");
        args.push(volume_id);
    }
    let mut need_alt = false;
    if let Some(boot_bios) = iso.boot_bios.as_deref() {
        args.push("-eltorito-boot");
        args.push(boot_bios);
        args.push("-no-emul-boot");
        args.push("-boot-info-table");
        need_alt = true;
    }
    if let Some(boot_uefi) = iso.boot_uefi.as_deref() {
        if need_alt {
            args.push("-eltorito-alt-boot");
        }
        args.push("-eltorito-platform");
        args.push("efi");
        args.push("-eltorito-boot");
        args.push(boot_uefi);
        args.push("-no-emul-boot");
    }
    args.push("-o");
    args.push(imagefile.to_str().unwrap());
    args.push(rmp.to_str().unwrap());
    ensure::run(&ib.log, &args)?;

    /*
     * If this is to be a hybrid ISO (i.e., can also be used as a USB boot disk)
     * we need to create a partition table that maps to the appropriate regions
     * of the ISO image.
     */
    if let Some(hybrid) = iso.hybrid.as_ref() {
        if iso.boot_uefi.is_none() {
            bail!("hybrid images must support UEFI at present");
        }

        /*
         * Attach the ISO image file as a labelled lofi device, so that we can
         * use fdisk(1M) and installboot(1M):
         */
        let lofi = attach_lofi(&ib.log, &imagefile, true)?;
        let rdev = lofi.rdevpath.as_ref().unwrap();

        /*
         * Create a small x86 boot partition (type 190) near the start of the
         * ISO, from sector 3 up to sector 63.  The installboot(1M) utility will
         * place the stage2 loader file (which can boot from the ISO portion of
         * the image) there.
         *
         * The ISO data itself begins at sector 64.
         */
        fdisk_add(&ib.log, rdev, 190, 3, 60)?;

        if iso.boot_uefi.is_some() {
            /*
             * Locate the EFI system partition image within the ISO file:
             */
            let esp = etdump(&ib.log, &imagefile, "efi", "i386")?;
            info!(ib.log, "esp @ {:?}", esp);

            /*
             * Create a partition table entry so that EFI firmware knows to
             * look for the ESP:
             */
            let start = (esp.offset as u32) / 512;
            let nsectors = (esp.length as u32) / 512;
            fdisk_add(&ib.log, rdev, 239, start, nsectors)?;
        }

        /*
         * The p0 device represents the whole disk.  Unfortunately, installboot
         * does not presently accept such a path; it requires the path to a file
         * system for which it can identify the type.  We choose the ESP, which
         * is the second partition in our MBR table.
         */
        let p0 = rdev.to_str().unwrap();
        let p2 = if let Some(rdsk) = p0.strip_suffix("p0") {
            format!("{}p2", rdsk)
        } else {
            bail!("unexpected lofi device path: {}", p0);
        };

        let stage1 = format!("{}/{}", rmp.to_str().unwrap(), hybrid.stage1);
        let stage2 = format!("{}/{}", rmp.to_str().unwrap(), hybrid.stage2);
        installboot(&ib.log, &p2, &stage1, &stage2)?;

        teardown_lofi(&ib.log, &imagefile)?;
    }

    /*
     * Copy the image file to the output directory.
     */
    let outputfile = ib.output_file(&format!("{}-{}.iso", ib.group,
        ib.output_name))?;

    info!(ib.log, "copying image {} to output file {}",
        imagefile.display(), outputfile.display());
    ensure::removed(&ib.log, &outputfile)?;
    std::fs::copy(&imagefile, &outputfile)?;
    ensure::perms(&ib.log, &outputfile, ROOT, ROOT, 0o644)?;

    info!(ib.log, "completed processing {}/{}", ib.group, ib.name);

    Ok(())
}

fn run_build_fs(ib: &mut ImageBuilder) -> Result<()> {
    let log = &ib.log;

    let fstyp = if let Some(ufs) = ib.template.ufs.as_ref() {
        Fstyp::Ufs(ufs.clone())
    } else if let Some(pcfs) = ib.template.pcfs.as_ref() {
        Fstyp::Pcfs(pcfs.clone())
    } else {
        bail!("expected a \"ufs\" or \"pcfs\" property");
    };

    /*
     * Arrange this structure:
     *  /ROOT_DATASET/group/name
     *      /lofi.$fsname -- the lofi image underpinning the UFS/FAT image
     *      /a -- UFS/FAT root mountpoint
     *
     * Steps like "ensure_file" will have their root directory relative to "/a".
     * This directory represents "/" (the root file system) in the target boot
     * environment.
     */

    /*
     * First, ensure there is no file system mounted at our expected location:
     */
    let rmp = ib.root()?;

    loop {
        let mounts = illumos::mounts()?
            .iter()
            .filter(|m| rmp == PathBuf::from(&m.mount_point))
            .cloned()
            .collect::<Vec<_>>();
        if mounts.is_empty() {
            break;
        }
        info!(log, "found old mounts: {:#?}", mounts);
        ensure::run(log, &["/sbin/umount", "-f", rmp.to_str().unwrap()])?;
    }
    info!(log, "nothing mounted at {:?}", rmp);

    let (fsname, size) = match &fstyp {
        Fstyp::Ufs(ufs) => ("ufs", ufs.size),
        Fstyp::Pcfs(pcfs) => ("pcfs", pcfs.size),
    };

    /*
     * Now, make sure we have a fresh lofi device...
     */
    let imagefile = ib.work_file(&format!("lofi.{}", fsname))?;
    info!(log, "image file: {}", imagefile.display());

    /*
     * Create a regular (unlabelled) lofi(7D) device.  We do not need to
     * manipulate slices to create the ramdisk image:
     */
    let lofi = recreate_lofi(log, &imagefile, size, false)?;
    let ldev = lofi.devpath.unwrap();
    let lrdev = lofi.rdevpath.unwrap();

    let mntopts = match &fstyp {
        Fstyp::Ufs(ufs) => {
            ensure::run(log, &["/usr/sbin/newfs", "-o", "space", "-m", "0",
                "-i", &ufs.inode_density.to_string(), "-b", "4096",
                ldev.to_str().unwrap()])?;
            "nologging,noatime"
        }
        Fstyp::Pcfs(pcfs) => {
            /*
             * Because we are using the "nofdisk" option, we need to specify the
             * target file system size in term of 512 byte sectors:
             */
            let secsize = size * 1024 * 1024 / 512;
            ensure::run(log, &["/usr/sbin/mkfs", "-F", "pcfs", "-o",
                &format!("b={},nofdisk,size={}", pcfs.label, secsize),
                lrdev.to_str().unwrap()])?;
            "noatime"
        }
    };

    ensure::directory(log, &rmp, ROOT, ROOT, 0o755)?;

    ensure::run(log, &["/usr/sbin/mount", "-F", fsname, "-o", mntopts,
        ldev.to_str().unwrap(), rmp.to_str().unwrap()])?;

    /*
     * Now that we have created the file system, run the steps for this
     * template.
     */
    run_steps(ib)?;

    info!(ib.log, "steps complete; finalising image!");

    /*
     * Report the used and available space in the temporary pool before we
     * export it.
     */
    if let Fstyp::Ufs(_) = &fstyp {
        /*
         * Only UFS has inodes.
         */
        ensure::run(&ib.log, &["/usr/bin/df", "-o", "i",
            rmp.to_str().unwrap()])?;
    }
    ensure::run(&ib.log, &["/usr/bin/df", "-h", rmp.to_str().unwrap()])?;

    /*
     * Unmount the file system and detach the lofi device.
     */
    ensure::run(&ib.log, &["/sbin/umount", rmp.to_str().unwrap()])?;
    lofi::lofi_unmap_device(&ldev)?;

    /*
     * Copy the image file to the output directory.
     */
    let outputfile = ib.output_file(&format!("{}-{}.{}", ib.group,
        ib.output_name, fsname))?;

    info!(ib.log, "copying image {} to output file {}",
        imagefile.display(), outputfile.display());
    ensure::removed(&ib.log, &outputfile)?;
    std::fs::copy(&imagefile, &outputfile)?;
    ensure::perms(&ib.log, &outputfile, ROOT, ROOT, 0o644)?;

    info!(ib.log, "completed processing {}/{}", ib.group, ib.name);

    Ok(())
}

fn run_build_pool(ib: &mut ImageBuilder) -> Result<()> {
    let log = &ib.log;

    let temppool = ib.temp_pool();

    /*
     * Arrange this structure:
     *  /ROOT_DATASET/group/name
     *      /lofi.raw -- the lofi image underpinning the pool
     *      /altroot -- pool altroot (zpool create -R)
     *      /a -- boot environment mount directory?
     *
     * Steps like "ensure_file" will have their root directory relative to "/a".
     * This directory represents "/" (the root file system) in the target boot
     * environment.
     *
     * The temporary pool name will be "TEMPORARY-$group-$name".
     */

    /*
     * First, destroy a pool if we have left one around...
     */
    pool_destroy(log, &temppool)?;

    /*
     * Now, make sure we have a fresh lofi device...
     */
    let imagefile = ib.work_file("lofi.raw")?;
    info!(log, "image file: {}", imagefile.display());

    let altroot = ib.work_file("altroot")?;
    info!(log, "pool altroot: {}", altroot.display());

    let pool = ib.template.pool.as_ref().unwrap();

    /*
     * Attach this file as a labelled lofi(7D) device so that we can manage
     * slices.
     */
    let lofi = recreate_lofi(log, &imagefile, pool.size(), true)?;
    let ldev = lofi.devpath.as_ref().unwrap();

    let disk = if pool.label() {
        ldev.to_str().unwrap().trim_end_matches("p0")
    } else {
        ldev.to_str().unwrap()
    };
    info!(log, "pool device = {}", disk);

    /*
     * Create the new pool, using the temporary pool name while it is imported
     * on this system.  We specify an altroot to avoid using the system cache
     * file, and to avoid mountpoint clashes with the system pool.  If we do not
     * explicitly set the mountpoint of the pool (create -m ...) then it will
     * default to the dynamically constructed "/$poolname", which will be
     * correct both on this system and on the target system when it is
     * eventually imported as its target name.
     */
    let compression = format!("compression={}", pool.compression());
    let mut args = vec![
        "/sbin/zpool", "create",
        "-t", &temppool,
        "-O", &compression,
        "-R", altroot.to_str().unwrap(),
    ];

    if pool.no_features() {
        /*
         * Create the pool without any enabled features.  This means it will be
         * compatible with the widest variety of OS versions.
         */
        args.push("-d");
    }

    if pool.uefi() {
        /*
         * If we need UEFI support, we must pass -B to create the
         * ESP slice.  Note that this consumes 256MB of space in the
         * image.
         */
        args.push("-B");
    }

    args.push("-o");
    let ashiftarg = format!("ashift={}", pool.ashift());
    args.push(&ashiftarg);

    if pool.autoexpand() {
        args.push("-o");
        args.push("autoexpand=on");
    }

    let targpool = ib.target_pool();
    args.push(&targpool);
    args.push(disk);

    ensure::run(log, args.as_slice())?;

    /*
     * Now that we have created the pool, run the steps for this template.
     */
    run_steps(ib)?;

    info!(ib.log, "steps complete; finalising image!");

    /*
     * Report the used and available space in the temporary pool before we
     * export it.
     */
    info!(ib.log, "temporary pool has {} used, {} avail, {} compressratio",
        zfs_get(&temppool, "used")?,
        zfs_get(&temppool, "avail")?,
        zfs_get(&temppool, "compressratio")?);

    /*
     * Export the pool and detach the lofi device.
     */
    pool_export(&ib.log, &temppool)?;

    if ib.template.pool.as_ref().unwrap().partition_only() {
        let outpartfile = ib.output_file(&format!("{}-{}.partonly", ib.group,
            ib.output_name))?;
        ensure::removed(&ib.log, &outpartfile)?;
        info!(ib.log, "extract just the ZFS partition to {:?}", outpartfile);

        let uefi = ib.template.pool.as_ref().unwrap().uefi.unwrap_or(false);
        let slice = if uefi {
            "1"
        } else {
            "0"
        };

        ensure::run(&ib.log, &[
            "dd",
            &format!("if={}s{}", disk.replace("dsk", "rdsk"), slice),
            &format!("of={}", outpartfile.to_str().unwrap()),
            "bs=1k",
        ])?;
    }

    lofi::lofi_unmap_device(&ldev)?;

    /*
     * Copy the image file to the output directory.
     */
    let outputfile = ib.output_file(&format!("{}-{}.raw", ib.group,
        ib.output_name))?;

    info!(ib.log, "copying image {} to output file {}",
        imagefile.display(), outputfile.display());
    ensure::removed(&ib.log, &outputfile)?;
    std::fs::copy(&imagefile, &outputfile)?;
    ensure::perms(&ib.log, &outputfile, ROOT, ROOT, 0o644)?;

    info!(ib.log, "completed processing {}/{}", ib.group, ib.name);

    Ok(())
}

fn find_template_root(arg: Option<String>) -> Result<PathBuf> {
    Ok(if let Some(arg) = arg {
        let p = PathBuf::from(&arg);
        if p.is_relative() {
            let mut cd = std::env::current_dir()?;
            cd.push(&p);
            cd
        } else {
            p
        }
    } else {
        /*
         * If no template root is specified, we default to the natural location:
         * either up one, if we are deployed in a "bin" directory, or up to the
         * project root if we reside in a Cargo "target" directory.
         */
        jmclib::dirs::rootpath("templates")?
    })
}

fn run_build(log: &Logger, mat: &getopts::Matches) -> Result<()> {
    let group = mat.opt_str("g").unwrap();
    let name = mat.opt_str("n").unwrap();
    let output_name = if let Some(n) = mat.opt_str("N") {
        /*
         * Allow the user to override the name we use for the work area and
         * output files, as distinct from the template name.
         */
        n
    } else {
        name.clone()
    };
    let ibrootds = mat.opt_str("d").unwrap();
    let fullreset = mat.opt_present("x");
    let reset = mat.opt_present("r");
    let template_root = find_template_root(mat.opt_str("T"))?;
    let external_src = mat.opt_strs("E")
        .iter()
        .map(|e| {
            let ee = PathBuf::from(e);
            if !ee.is_dir() {
                bail!("external file source {:?} should be a directory", e);
            }
            Ok(ee)
        }).collect::<Result<Vec<PathBuf>>>()?;

    /*
     * Process feature directives in the order in which they appear.  Directives
     * can override the value of previous definitions, and can remove a feature
     * already defined, to hopefully easily enable argument lists to be
     * constructed in control programs that override default values.
     */
    let mut features = Features::default();
    mat.opt_strs("F")
        .iter()
        .map(|o| {
            let o = o.trim();

            Ok(if o.is_empty() {
                bail!("-F requires an argument");
            } else if o.starts_with('^') {
                /*
                 * This is a negated feature; i.e., -F ^BLAH
                 */
                if o.contains('=') {
                    bail!("-F with a negated feature cannot have a value");
                }
                (o.chars().skip(1).collect::<String>(), None)
            } else if let Some((f, v)) = o.split_once('=') {
                /*
                 * This is an enabled feature with a value; i.e., -F BLAH=YES
                 */
                (f.trim().to_string(), Some(v.to_string()))
            } else {
                /*
                 * This is an enabled feature with no value; i.e., -F BLAH
                 * Just set it to "1", as if the user had passed: -F BLAH=1
                 */
                (o.to_string(), Some("1".to_string()))
            })
        }).collect::<Result<Vec<_>>>()?
        .iter()
        .for_each(|(f, v)| {
            if let Some(v) = v {
                features.set(f, v);
            } else {
                features.clear(f);
            }
        });

    /*
     * Using the system svccfg(1M) is acceptable under some conditions, but not
     * all.  This is only safe as long as the service bundle DTD, and the
     * service bundles in the constructed image, as well as the target
     * repository database file format, are compatible with the build system
     * svccfg.  Sadly it is also not generally correct to use the svccfg program
     * from the image under construction, as it may depend on newer libc or
     * kernel or other private interfaces than are available on the build
     * system.
     *
     * If a new feature is required, then the "svccfg-native" binary from the
     * built illumos tree will be required.  Indeed, for a completely robust
     * image build process, that program should always be used.
     */
    let svccfg = mat.opt_str("S")
        .unwrap_or_else(|| "/usr/sbin/svccfg".to_string());

    let t = load_template(log, &template_root, &group,
        Load::Main(group.to_string(), name.to_string()), &features)
        .context(format!("loading template {}:{}", group, name))?;

    if !dataset_exists(&ibrootds)? {
        bail!("image builder root dataset \"{}\" does not exist", ibrootds);
    }

    /*
     * Create a dataset to hold the output files of various builds.
     */
    let outputds = format!("{}/output", ibrootds);
    if !dataset_exists(&outputds)? {
        dataset_create(log, &outputds, false)?;
    }

    let tmpds = format!("{}/tmp/{}/{}", ibrootds, group, output_name);
    info!(log, "temporary dataset: {}", tmpds);
    if dataset_exists(&tmpds)? {
        dataset_remove(log, &tmpds)?;
    }
    dataset_create(log, &tmpds, true)?;
    zfs_set(log, &tmpds, "sync", "disabled")?;
    let tmpdir = zfs_get(&tmpds, "mountpoint")?;
    info!(log, "temporary directory: {}", tmpdir);

    /*
     * XXX Generate a unique bename.  This is presently necessary because beadm
     * does not accept an altroot (-R) flag, and thus the namespace for boot
     * environments overlaps between "real" boot environments in use on the host
     * and any we create on the target image while it is mounted.
     *
     * Ideally, this will go away with changes to illumos.
     */
    let mut bename = Uuid::new_v4().to_hyphenated().to_string()[0..8]
        .to_string();

    let c = t.ufs.is_some() as u32
        + t.pcfs.is_some() as u32
        + t.iso.is_some() as u32
        + t.pool.is_some() as u32
        + t.dataset.is_some() as u32;
    if c > 1 {
        bail!("template must have at most one of \"dataset\", \"ufs\", \
            \"pool\", or \"iso\"");
    }

    if t.dataset.is_none() {
        let workds = format!("{}/work/{}/{}", ibrootds, group, output_name);
        info!(log, "work dataset: {}", workds);

        if !dataset_exists(&workds)? {
            /*
             * For pool jobs, we just create the target dataset.  The temporary
             * pool will be destroyed and created as a lofi image inside this
             * dataset, providing the required idempotency.
             */
            dataset_create(log, &workds, true)?;
        }

        /*
         * We can disable sync on the work dataset to make writes to the lofi
         * device go a lot faster.  If the system crashes, we're going to start
         * this build again anyway.  Output files are stored in a different
         * dataset where sync is not disabled.  It would be tempting to put the
         * image in the temporary dataset rather than the work dataset, but we
         * destroy that unconditionally for each run and if we left a lingering
         * lofi device open that was attached in the temporary dataset that
         * would fail.
         */
        zfs_set(log, &workds, "sync", "disabled")?;

        let (build_type, func) = if let Some(pool) = &t.pool {
            if let Some(n) = pool.bename() {
                bename = n.to_string();
            }
            (BuildType::Pool(pool.name().to_string()), run_build_pool as Build)
        } else if t.ufs.is_some() {
            (BuildType::Ufs, run_build_fs as Build)
        } else if t.pcfs.is_some() {
            (BuildType::Pcfs, run_build_fs as Build)
        } else if t.iso.is_some() {
            (BuildType::Iso, run_build_iso as Build)
        } else {
            panic!("expected one or the other");
        };

        let mut ib = ImageBuilder {
            build_type,
            bename,
            group,
            name,
            output_name,
            template_root,
            template: t,
            workds,
            outputds,
            tmpds,
            svccfg,
            features,
            external_src,
            log: log.clone(),
        };

        (func)(&mut ib)?;

        /*
         * Work datasets for builds that result in image files can be safely
         * removed after a successful build as they are not re-used in
         * subsequent builds.  A failure to remove the dataset at this stage
         * would imply we did not clean up all of the lofi devices, etc.
         */
        dataset_remove(log, &ib.workds)?;
        dataset_remove(log, &ib.tmpds)?;

        return Ok(());
    }

    let dataset = t.dataset.as_ref().unwrap();
    let dataset_name = features.expand(&dataset.name)?;

    let workds = format!("{}/work/{}/{}", ibrootds, group, dataset_name);
    info!(log, "work dataset: {}", workds);

    let mut ib = ImageBuilder {
        build_type: BuildType::Dataset,
        bename,
        group: group.clone(),
        name: name.clone(),
        output_name,
        template_root,
        template: t.clone(),
        workds: workds.clone(),
        outputds,
        tmpds,
        svccfg,
        features,
        external_src,
        log: log.clone(),
    };

    let input_snapshot = ib.expando(dataset.input_snapshot.as_deref())?;
    let output_snapshot = ib.expando(dataset.output_snapshot.as_deref())?;

    if fullreset {
        info!(log, "resetting by removing work dataset: {}", workds);
        dataset_remove(log, &workds)?;
    }

    if dataset_exists(&workds)? {
        /*
         * The dataset exists already.  If this template has configured an
         * output snapshot, we can roll back to it without doing any more work.
         */
        if let Some(snap) = output_snapshot.as_deref() {
            info!(log, "looking for output snapshot {}@{}", workds, snap);
            if snapshot_exists(&workds, snap)? && !reset {
                snapshot_rollback(log, &workds, snap)?;
                info!(log, "rolled back to output snapshot; \
                    no work required");
                return Ok(());
            }

            if input_snapshot.is_none() {
                /*
                 * If there is no input snapshot, we do not know how to make
                 * the dataset pristine for this build.  Bail out.
                 */
                bail!("the dataset exists, but an input snapshot was not \
                    specified and the output snapshot does not exist; \
                    a full reset is required");
            }
        }

        /*
         * If an input snapshot was specified, then we know how to reset the
         * dataset to a pristine state at which this build may begin.
         */
        if let Some(snap) = input_snapshot.as_deref() {
            info!(log, "looking for input snapshot {}@{}", workds, snap);
            if snapshot_exists(&workds, snap)? {
                snapshot_rollback(log, &workds, snap)?;
                info!(log, "rolled back to input snapshot; work may begin");
            } else {
                bail!("the dataset exists, but the specified input snapshot \
                    does not; a full reset is required");
            }
        } else {
            assert!(output_snapshot.is_none());
            assert!(input_snapshot.is_none());

            bail!("the dataset exists, but neither an input nor an output \
                snapshot was specified; a full reset is required");
        }

        assert!(input_snapshot.is_some());
    }

    dataset_create(log, &workds, true)?;

    run_steps(&mut ib)?;

    if let Some(snap) = output_snapshot.as_deref() {
        info!(log, "creating output snapshot {}@{}", workds, snap);
        snapshot_create(log, &workds, snap)?;
    }

    /*
     * Remove the temporary dataset we created for this build.  Note that we do
     * not remove the work dataset, as multiple file-tree builds will act on
     * that dataset in sequence.
     */
    dataset_remove(log, &ib.tmpds)?;

    info!(log, "completed processing {}/{}", group, name);

    Ok(())
}

fn gzip<P1, P2>(log: &Logger, src: P1, dst: P2) -> Result<()>
    where P1: AsRef<Path>, P2: AsRef<Path>
{
    let src = src.as_ref();
    let dst = dst.as_ref();

    info!(log, "GZIP {:?} -> {:?}", src, dst);

    let f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(dst)?;

    let cmd = Command::new("/usr/bin/gzip")
        .env_clear()
        .arg("-c")
        .arg(src)
        .stdout(Stdio::from(f))
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("gzip {:?} > {:?} failed: {}", src, dst, errmsg);
    }

    info!(log, "gzip ok");

    Ok(())
}

fn zpool_set(log: &Logger, pool: &str, n: &str, v: &str) -> Result<()> {
    if pool.contains('/') {
        bail!("no / allowed here");
    }

    info!(log, "SET POOL PROPERTY ON {}: {} = {}", pool, n, v);

    let cmd = Command::new("/sbin/zpool")
        .env_clear()
        .arg("set")
        .arg(&format!("{}={}", n, v))
        .arg(pool)
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("zpool set {} failed: {}", n, errmsg);
    }

    Ok(())
}

fn zfs_set(log: &Logger, dataset: &str, n: &str, v: &str) -> Result<()> {
    info!(log, "SET DATASET PROPERTY ON {}: {} = {}", dataset, n, v);

    let cmd = Command::new("/sbin/zfs")
        .env_clear()
        .arg("set")
        .arg(&format!("{}={}", n, v))
        .arg(dataset)
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("zfs set {} failed: {}", n, errmsg);
    }

    Ok(())
}

fn zfs_get(dataset: &str, n: &str) -> Result<String> {
    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("get")
        .arg("-H")
        .arg("-o").arg("value")
        .arg(n)
        .arg(dataset)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        bail!("zfs get failed: {}", errmsg);
    }

    let out = String::from_utf8(zfs.stdout)?;
    Ok(out.trim().to_string())
}

fn dataset_exists(dataset: &str) -> Result<bool> {
    if dataset.contains('@') {
        bail!("no @ allowed here");
    }

    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("list")
        .arg("-Ho").arg("name")
        .arg(dataset)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        if errmsg.trim().ends_with("dataset does not exist") {
            return Ok(false);
        }
        bail!("zfs list failed: {}", errmsg);
    }

    Ok(true)
}

fn dataset_remove(log: &Logger, dataset: &str) -> Result<bool> {
    if dataset.contains('@') {
        bail!("no @ allowed here");
    }

    info!(log, "DESTROY DATASET: {}", dataset);

    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("destroy")
        .arg("-r")
        .arg(dataset)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        if errmsg.trim().ends_with("dataset does not exist") {
            return Ok(false);
        }
        bail!("zfs destroy failed: {}", errmsg);
    }

    Ok(true)
}

fn pool_destroy(log: &Logger, name: &str) -> Result<bool> {
    if name.contains('@') {
        bail!("no @ allowed here");
    }

    info!(log, "DESTROY POOL: {}", name);

    let cmd = Command::new("/sbin/zpool")
        .env_clear()
        .arg("destroy")
        .arg("-f")
        .arg(&name)
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        if errmsg.trim().ends_with("no such pool") {
            return Ok(false);
        }
        bail!("zpool destroy failed: {}", errmsg);
    }

    Ok(true)
}

fn pool_export(log: &Logger, name: &str) -> Result<bool> {
    if name.contains('@') {
        bail!("no @ allowed here");
    }

    info!(log, "EXPORT POOL: {}", name);

    loop {
        let cmd = Command::new("/sbin/zpool")
            .env_clear()
            .arg("export")
            .arg(&name)
            .output()?;

        if cmd.status.success() {
            break;
        }

        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        if errmsg.trim().ends_with("pool is busy") {
            warn!(log, "pool is busy... retrying...");
            std::thread::sleep(std::time::Duration::from_secs(1));
            continue;
        }
        bail!("zpool export failed: {}", errmsg);
    }

    Ok(true)
}

#[allow(dead_code)]
fn snapshot_remove(dataset: &str, snapshot: &str) -> Result<bool> {
    if dataset.contains('@') || snapshot.contains('@') {
        bail!("no @ allowed here");
    }

    let n = format!("{}@{}", dataset, snapshot);
    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("destroy")
        .arg(&n)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        if errmsg.trim().ends_with("dataset does not exist") {
            return Ok(false);
        }
        bail!("zfs list failed: {}", errmsg);
    }

    Ok(true)
}

fn snapshot_exists(dataset: &str, snapshot: &str) -> Result<bool> {
    if dataset.contains('@') || snapshot.contains('@') {
        bail!("no @ allowed here");
    }

    let n = format!("{}@{}", dataset, snapshot);
    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("list")
        .arg("-t").arg("snapshot")
        .arg("-Ho").arg("name")
        .arg(&n)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        if errmsg.trim().ends_with("dataset does not exist") {
            return Ok(false);
        }
        bail!("zfs list failed: {}", errmsg);
    }

    Ok(true)
}

fn snapshot_create(log: &Logger, dataset: &str, snapshot: &str)
    -> Result<bool>
{
    if dataset.contains('@') || snapshot.contains('@') {
        bail!("no @ allowed here");
    }

    let n = format!("{}@{}", dataset, snapshot);
    info!(log, "CREATE SNAPSHOT: {}", n);

    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("snapshot")
        .arg(&n)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        bail!("zfs snapshot failed: {}", errmsg);
    }

    Ok(true)
}

fn snapshot_rollback(log: &Logger, dataset: &str, snapshot: &str)
    -> Result<bool>
{
    if dataset.contains('@') || snapshot.contains('@') {
        bail!("no @ allowed here");
    }

    let n = format!("{}@{}", dataset, snapshot);
    info!(log, "ROLLBACK TO SNAPSHOT: {}", n);

    let zfs = Command::new("/sbin/zfs")
        .env_clear()
        .arg("rollback")
        .arg("-r")
        .arg(&n)
        .output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        bail!("zfs snapshot failed: {}", errmsg);
    }

    Ok(true)
}

fn dataset_create(log: &Logger, dataset: &str, parents: bool) -> Result<()> {
    if dataset.contains('@') {
        bail!("no @ allowed here");
    }

    info!(log, "CREATE DATASET: {}", dataset);

    let mut cmd = Command::new("/sbin/zfs");
    cmd.env_clear();
    cmd.arg("create");
    if parents {
        cmd.arg("-p");
    }
    cmd.arg(dataset);

    let zfs = cmd.output()?;

    if !zfs.status.success() {
        let errmsg = String::from_utf8_lossy(&zfs.stderr);
        bail!("zfs create failed: {}", errmsg);
    }

    Ok(())
}

fn mkfile<P: AsRef<Path>>(log: &Logger, filename: P, mblen: usize)
    -> Result<()>
{
    let filename = filename.as_ref();
    info!(log, "CREATE IMAGE ({}MB): {}", mblen, filename.display());

    let cmd = Command::new("/usr/sbin/mkfile")
        .env_clear()
        .arg(&format!("{}m", mblen))
        .arg(filename.as_os_str())
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("mkfile({}) failed: {}", filename.display(), errmsg);
    }

    Ok(())
}

fn pkg(log: &Logger, args: &[&str]) -> Result<()> {
    let mut newargs = vec!["/usr/bin/pkg"];
    for arg in args {
        newargs.push(arg);
    }

    ensure::run(log, &newargs)
}

fn pkg_install(
    log: &Logger,
    root: &str,
    packages: &[impl AsRef<str>]
) -> Result<()> {
    let mut newargs = vec!["/usr/bin/pkg", "-R", root, "install"];
    for pkg in packages {
        newargs.push(pkg.as_ref());
    }

    ensure::run(log, &newargs)
}

fn pkg_uninstall(log: &Logger, root: &str, packages: &[&str]) -> Result<()> {
    let mut newargs = vec!["/usr/bin/pkg", "-R", root, "uninstall"];
    for pkg in packages {
        newargs.push(pkg);
    }

    ensure::run(log, &newargs)
}

fn pkg_optional_deps(_log: &Logger, root: &str, package: &str,
    strip_publisher: bool) -> Result<Vec<String>>
{
    let cmd = Command::new("/usr/bin/pkg")
        .env_clear()
        .arg("-R").arg(root)
        .arg("contents")
        .arg("-t").arg("depend")
        .arg("-a").arg("type=optional")
        .arg("-H")
        .arg("-o").arg("fmri")
        .arg(package)
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("pkg contents failed: {}", errmsg);
    }

    let out = String::from_utf8(cmd.stdout)?;
    Ok(out
        .lines()
        .map(|s| fmri::Package::parse_fmri(s))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .map(|p| if strip_publisher {
            p.to_string_without_publisher()
        } else {
            p.to_string()
        })
        .collect())
}

fn pkg_ensure_variant(log: &Logger, root: &str, variant: &str, value: &str)
    -> Result<()>
{
    let cmd = Command::new("/usr/bin/pkg")
        .env_clear()
        .arg("-R").arg(root)
        .arg("variant")
        .arg("-F").arg("json")
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("pkg variant failed: {}", errmsg);
    }

    #[derive(Deserialize)]
    struct Variant {
        variant: String,
        value: String,
    }

    let tab: Vec<Variant> = serde_json::from_slice(&cmd.stdout)?;
    for ent in tab.iter() {
        if ent.variant == format!("variant.{}", variant) {
            if ent.value == value {
                info!(log, "variant {} is already {}", variant, value);
                return Ok(());
            } else {
                info!(log, "variant {} is {}; changing to {}", variant,
                    ent.value, value);
                break;
            }
        }
    }

    ensure::run(log, &["/usr/bin/pkg", "-R", root, "change-variant",
        &format!("{}={}", variant, value)])?;
    Ok(())
}

fn pkg_ensure_facet(log: &Logger, root: &str, facet: &str, value: &str)
    -> Result<()>
{
    let cmd = Command::new("/usr/bin/pkg")
        .env_clear()
        .arg("-R").arg(root)
        .arg("facet")
        .arg("-F").arg("json")
        .output()?;

    if !cmd.status.success() {
        let errmsg = String::from_utf8_lossy(&cmd.stderr);
        bail!("pkg facet failed: {}", errmsg);
    }

    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct Facet {
        facet: String,
        masked: String,
        src: String,
        value: String,
    }

    let tab: Vec<Facet> = serde_json::from_slice(&cmd.stdout)?;
    for ent in tab.iter() {
        if ent.facet == format!("facet.{}", facet) {
            if ent.value == value {
                info!(log, "facet {} is already {}", facet, value);
                return Ok(());
            } else {
                info!(log, "facet {} is {}; changing to {}", facet,
                    ent.value, value);
                break;
            }
        }
    }

    ensure::run(log, &["/usr/bin/pkg", "-R", root, "change-facet",
        &format!("{}={}", facet, value)])?;
    Ok(())
}

fn seed_smf(log: &Logger, svccfg: &str, tmpdir: &Path, mountpoint: &Path,
    debug: bool, apply_site: bool, seed: Option<&str>) -> Result<()>
{
    let tmpdir = tmpdir.to_str().unwrap();
    let mountpoint = mountpoint.to_str().unwrap();

    let dtd = format!("{}/usr/share/lib/xml/dtd/service_bundle.dtd.1",
        mountpoint);
    let repo = format!("{}/repo.db", tmpdir);
    let manifests = format!("{}/lib/svc/manifest", mountpoint);
    let installto = format!("{}/etc/svc/repository.db", mountpoint);

    if let Some(p) = seed {
        let seeddb = format!("{}/lib/svc/seed/{}.db", mountpoint, p);
        ensure::file(log, &seeddb, &repo, ROOT, ROOT, 0o600, Create::Always)?;
    } else {
        ensure::removed(log, &repo)?;
    }

    let mut env = HashMap::new();
    env.insert("SVCCFG_DTD".to_string(), dtd);
    env.insert("SVCCFG_REPOSITORY".to_string(), repo.to_string());
    env.insert("SVCCFG_CHECKHASH".to_string(), "1".to_string());
    env.insert("PKG_INSTALL_ROOT".to_string(), mountpoint.to_string());

    ensure::run_envs(log,
        &[svccfg, "import", "-p", "/dev/stdout", &manifests], Some(&env))?;

    /*
     * If required, smf(5) can generate quite a lot of debug log output.  This
     * output includes diagnostic information about transitions in the service
     * graph, the execution of methods, the management of contracts, etc.
     *
     * This extra debugging can be requested in the boot arguments, via "-m
     * debug", but that presently forces the output to the console which is
     * generally unhelpful.  It is also not possible to set boot arguments
     * in an environment such as AWS where we have no control over the
     * instance console.  Fortunately the logs can be enabled a second way:
     * through smf(5) properties.
     *
     * If requested enable debug logging for this image:
     */
    if debug {
        ensure::run_envs(log, &[svccfg, "-s", "system/svc/restarter:default",
            "addpg", "options", "application"], Some(&env))?;
        ensure::run_envs(log, &[svccfg, "-s", "system/svc/restarter:default",
            "setprop", "options/logging=debug"], Some(&env))?;
    }

    /*
     * If the image ships a site profile, we may wish to apply it before the
     * first boot.  Otherwise, services that are disabled in the site profile
     * may start up before the profile is applied in the booted system, only to
     * then be disabled again.
     */
    if apply_site {
        let profile_site = format!("{}/var/svc/profile/site.xml", mountpoint);
        ensure::run_envs(log, &[svccfg, "apply", &profile_site], Some(&env))?;
    }

    ensure::file(log, &repo, &installto, ROOT, ROOT, 0o600,
        Create::Always)?;
    ensure::removed(log, &repo)?;

    Ok(())
}

#[derive(Clone, PartialEq)]
struct ShadowFile {
    entries: Vec<Vec<String>>,
}

impl ShadowFile {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut f = std::fs::File::open(path.as_ref())?;
        let mut data = String::new();
        f.read_to_string(&mut data)?;

        let entries = data.lines().enumerate().map(|(i, l)| {
            let fields = l.split(':').map(str::to_string).collect::<Vec<_>>();
            if fields.len() != 9 {
                bail!("invalid shadow line {}: {:?}", i, fields);
            }
            Ok(fields)
        }).collect::<Result<Vec<_>>>()?;

        Ok(ShadowFile {
            entries,
        })
    }

    pub fn password_set(&mut self, user: &str, password: &str) -> Result<()> {
        /*
         * First, make sure the username appears exactly once in the shadow
         * file.
         */
        let mc = self.entries.iter().filter(|e| e[0] == user).count();
        if mc != 1 {
            bail!("found {} matches for user {} in shadow file", mc, user);
        }

        self.entries.iter_mut().for_each(|e| {
            if e[0] == user {
                e[1] = password.to_string();
            }
        });
        Ok(())
    }

    pub fn write<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path.as_ref())?;

        let mut data = self.entries.iter().map(|e| e.join(":"))
            .collect::<Vec<_>>()
            .join("\n");
        data.push('\n');

        f.write_all(data.as_bytes())?;
        f.flush()?;
        Ok(())
    }
}

enum Fstyp {
    Ufs(Ufs),
    Pcfs(Pcfs),
}

enum BuildType {
    Pool(String),
    Dataset,
    Ufs,
    Iso,
    Pcfs,
}

#[derive(Default)]
struct Features {
    features: HashMap<String, String>,
}

impl Features {
    fn set<N: AsRef<str>, V: AsRef<str>>(&mut self, name: N, value: V) {
        let name = name.as_ref().to_string();
        let value = value.as_ref().to_string();

        self.features.insert(name, value);
    }

    fn clear<N: AsRef<str>>(&mut self, name: N) {
        self.features.remove(name.as_ref());
    }

    fn with<N: AsRef<str>>(&self, name: N) -> bool {
        self.features.contains_key(name.as_ref())
    }

    fn expando(&self, value: Option<&str>) -> Result<Option<String>> {
        value.map(|value| self.expand(value)).transpose()
    }

    fn expand(&self, value: &str) -> Result<String> {
        Ok(Expansion::parse(value)?.evaluate(&self.features)?)
    }
}

struct ImageBuilder {
    build_type: BuildType,

    group: String,
    name: String,
    output_name: String,

    log: Logger,
    template_root: PathBuf,
    workds: String,
    outputds: String,
    tmpds: String,
    template: Template,
    bename: String,

    svccfg: String,

    features: Features,
    external_src: Vec<PathBuf>,
}

impl ImageBuilder {
    /**
     * The root directory of the target system image.  This is the work dataset
     * mountpoint in the case of a dataset build, or the /a directory underneath
     * that for a pool build.
     */
    fn root(&self) -> Result<PathBuf> {
        let s = zfs_get(&self.workds, "mountpoint")?;
        let t = zfs_get(&self.tmpds, "mountpoint")?;

        Ok(PathBuf::from(match &self.build_type {
            BuildType::Dataset => s,
            /*
             * When a template targets a pool, files target the mountpoint of
             * the boot environment we create in the pool.
             */
            BuildType::Pool(_) => s + "/a",
            /*
             * When a template targets a UFS or a FAT file system, files target
             * the mountpoint of the file system we create on the lofi device.
             */
            BuildType::Ufs | BuildType::Pcfs => s + "/a",
            /*
             * ISO Files are not created using a lofi device, so we can just
             * use the temporary dataset.
             */
            BuildType::Iso => t + "/proto",
        }))
    }

    fn target_pool(&self) -> String {
        if let BuildType::Pool(name) = &self.build_type {
            name.to_string()
        } else {
            panic!("not a pool job");
        }
    }

    /**
     * The name of the pool as it is imported on the build machine.  This name
     * is ephemeral; the target pool name is the "real" name of the pool (e.g.,
     * "rpool") as it will appear on the installed host.
     */
    fn temp_pool(&self) -> String {
        if let BuildType::Pool(_) = &self.build_type {
            format!("TEMPORARY-{}-{}", self.group, self.output_name)
        } else {
            panic!("not a pool job");
        }
    }

    fn tmpdir(&self) -> Result<PathBuf> {
        let s = zfs_get(&self.tmpds, "mountpoint")?;
        Ok(PathBuf::from(s))
    }

    fn tmp_file(&self, n: &str) -> Result<PathBuf> {
        let mut p = PathBuf::from(zfs_get(&self.tmpds, "mountpoint")?);
        p.push(n);
        Ok(p)
    }

    fn work_file(&self, n: &str) -> Result<PathBuf> {
        let mut p = PathBuf::from(zfs_get(&self.workds, "mountpoint")?);
        p.push(n);
        Ok(p)
    }

    fn output_file(&self, n: &str) -> Result<PathBuf> {
        let mut p = PathBuf::from(zfs_get(&self.outputds, "mountpoint")?);
        p.push(n);
        Ok(p)
    }

    fn external_src_file(&self, filename: &str) -> Result<PathBuf> {
        for dir in self.external_src.iter() {
            let mut s = dir.clone();
            s.push(filename);
            if let Some(fi) = ensure::check(&s)? {
                if !fi.is_file() {
                    bail!("template file {} is wrong type: {:?}", s.display(),
                        fi.filetype);
                }
                return Ok(s);
            }
        }

        bail!("could not find external source file \"{}\"", filename);
    }

    fn template_file(&self, filename: &str) -> Result<PathBuf> {
        /*
         * First, try in the group-specific directory:
         */
        let mut s = self.template_root.clone();
        s.push(&format!("{}/files/{}", self.group, filename));
        if let Some(fi) = ensure::check(&s)? {
            if !fi.is_file() {
                bail!("template file {} is wrong type: {:?}", s.display(),
                    fi.filetype);
            }
            return Ok(s);
        }

        /*
         * Otherwise, fall back to the global directory:
         */
        let mut s = self.template_root.clone();
        s.push(&format!("files/{}", filename));
        if let Some(fi) = ensure::check(&s)? {
            if !fi.is_file() {
                bail!("template file {} is wrong type: {:?}", s.display(),
                    fi.filetype);
            }
            return Ok(s);
        }

        bail!("could not find template file \"{}\"", filename);
    }

    fn bename(&self) -> &str {
        &self.bename
    }

    fn feature_enabled(&self, name: &str) -> bool {
        self.features.with(name)
    }

    fn expando(&self, value: Option<&str>) -> Result<Option<String>> {
        self.features.expando(value)
    }

    fn expand(&self, value: &str) -> Result<String> {
        self.features.expand(value)
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Iso {
    boot_bios: Option<String>,
    boot_uefi: Option<String>,
    volume_id: Option<String>,
    hybrid: Option<Hybrid>,
}

#[derive(Deserialize, Debug, Clone)]
struct Hybrid {
    stage1: String,
    stage2: String,
}

#[derive(Deserialize, Debug, Clone)]
struct Ufs {
    size: usize,
    inode_density: usize,
}

#[derive(Deserialize, Debug, Clone)]
struct Pcfs {
    label: String,
    size: usize,
}

#[derive(Deserialize, Debug, Clone)]
struct Pool {
    name: String,
    bename: Option<String>,
    ashift: Option<u8>,
    uefi: Option<bool>,
    size: usize,
    partition_only: Option<bool>,
    no_features: Option<bool>,
    compression: Option<String>,
    label: Option<bool>,
    autoexpand: Option<bool>,
}

impl Pool {
    fn name(&self) -> &str {
        &self.name
    }

    fn uefi(&self) -> bool {
        /*
         * Default to no EFI System Partition (zpool create -B).
         */
        self.uefi.unwrap_or(false)
    }

    fn bename(&self) -> Option<&str> {
        self.bename.as_deref()
    }

    fn ashift(&self) -> u8 {
        /*
         * Default to 512 byte sectors.
         */
        self.ashift.unwrap_or(9)
    }

    fn size(&self) -> usize {
        self.size
    }

    fn partition_only(&self) -> bool {
        if !self.label() {
            /*
             * There are no partitions if we do not use a labelled lofi device.
             */
            false
        } else {
            self.partition_only.unwrap_or(false)
        }
    }

    fn no_features(&self) -> bool {
        self.no_features.unwrap_or(true)
    }

    fn label(&self) -> bool {
        self.label.unwrap_or(true)
    }

    fn compression(&self) -> &str {
        self.compression.as_deref().unwrap_or("on")
    }

    fn autoexpand(&self) -> bool {
        self.autoexpand.unwrap_or(false)
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Dataset {
    name: String,
    output_snapshot: Option<String>,
    input_snapshot: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
struct Step {
    #[serde(skip, default)]
    f: String,
    t: String,
    #[serde(default)]
    with: Option<String>,
    #[serde(default)]
    without: Option<String>,
    #[serde(flatten)]
    extra: serde_json::Value,
}

trait StepExt<T>
    where for<'de> T: Deserialize<'de>
{
    fn args(&self) -> Result<T>;
}

impl<T> StepExt<T> for Step
    where for<'de> T: Deserialize<'de>
{
    fn args(&self) -> Result<T> {
        Ok(serde_json::from_value(self.extra.clone())?)
    }
}

#[derive(Deserialize, Debug, Clone)]
struct Template {
    dataset: Option<Dataset>,
    pool: Option<Pool>,
    ufs: Option<Ufs>,
    pcfs: Option<Pcfs>,
    iso: Option<Iso>,
    steps: Vec<Step>,
}

fn include_path<P: AsRef<Path>>(root: P, group: &str, name: &str)
    -> Result<PathBuf>
{
    let paths = vec![
        format!("{}/include/{}.json", group, name),
        format!("include/{}.json", name),
    ];

    for p in paths.iter() {
        let mut fp = root.as_ref().to_path_buf();
        fp.push(p);

        if let Some(fi) = ensure::check(&fp)? {
            if fi.is_file() {
                return Ok(fp)
            }
        }
    }

    bail!("could not find include file in: {:?}", paths);
}

enum Load {
    Main(String, String),
    Include(String, String),
    IncludeExplicit(PathBuf),
}

impl Load {
    fn is_include(&self) -> bool {
        match self {
            Load::Include(_, _) | Load::IncludeExplicit(_) => true,
            Load::Main(_, _) => false,
        }
    }
}

fn load_template<P>(log: &Logger, root: P, group: &str, load: Load,
    features: &Features)
    -> Result<Template>
    where P: AsRef<Path>,
{
    let path = match &load {
        Load::Include(group, name) => {
            include_path(root.as_ref(), group, name)?
        }
        Load::Main(group, name) => {
            let mut path = root.as_ref().to_path_buf();
            path.push(format!("{}/{}.json", group, name));
            path
        }
        Load::IncludeExplicit(path) => {
            path.to_path_buf()
        }
    };

    let f = std::fs::File::open(&path)
        .with_context(|| anyhow!("template load path: {:?}", &path))?;
    let mut t: Template = serde_json::from_reader(f)?;
    if load.is_include() {
        if t.pool.is_some() || t.dataset.is_some() {
            bail!("cannot specify \"pool\" or \"dataset\" in an include");
        }
    } else {
        if t.pool.is_some() && t.dataset.is_some() {
            bail!("cannot specify both \"pool\" and \"dataset\" in a template");
        }
    }

    /*
     * Walk through the steps and expand any "include" entries before we
     * proceed.
     */
    let mut steps: Vec<Step> = Vec::new();
    for mut step in t.steps {
        if step.t == "include" {
            #[derive(Deserialize)]
            struct IncludeArgs {
                name: String,
                file: Option<String>,
            }

            let a: IncludeArgs = step.args()?;

            /*
             * If this include is dependent on being with or without a
             * particular feature, check for the present of that feature:
             */
            if let Some(feature) = step.with.as_deref() {
                if !features.with(feature) {
                    info!(log, "skip include {:?} because feature {:?} is \
                        not enabled", a.name, feature);
                    continue;
                }
            }
            if let Some(feature) = step.without.as_deref() {
                if features.with(feature) {
                    info!(log, "skip include {:?} because feature {:?} is \
                        enabled", a.name, feature);
                    continue;
                }
            }

            /*
             * Expand any feature macros that appear in the explicit include
             * file path if one was provided.  Care is taken to perform this
             * expansion after any feature guarding ("with") above, in case the
             * expansion would then use an undefined feature by mistake.
             */
            let file = features.expando(a.file.as_deref())?;
            let load = if let Some(file) = file.as_deref() {
                if !file.starts_with('/') {
                    bail!("file must be fully qualified path");
                }
                Load::IncludeExplicit(PathBuf::from(file))
            } else {
                Load::Include(group.to_string(), a.name.to_string())
            };

            let ti = load_template(log, root.as_ref(), group, load, features)?;
            for step in ti.steps {
                steps.push(step);
            }
        } else {
            step.f = path.clone().to_string_lossy().to_string();
            steps.push(step);
        }
    }

    t.steps = steps;

    Ok(t)
}

fn run_steps(ib: &mut ImageBuilder) -> Result<()> {
    let log = &ib.log;

    for (count, step) in ib.template.steps.iter().enumerate() {
        info!(log, "STEP {}: {}", count, step.t; "from" => &step.f);

        /*
         * If this step is dependent on being with or without a particular
         * feature, check for the present of that feature:
         */
        if let Some(feature) = step.with.as_deref() {
            if !ib.feature_enabled(feature) {
                info!(log, "skip step because feature {:?} is not enabled",
                    feature);
                continue;
            }
        }
        if let Some(feature) = step.without.as_deref() {
            if ib.feature_enabled(feature) {
                info!(log, "skip step because feature {:?} is enabled",
                    feature);
                continue;
            }
        }

        match step.t.as_str() {
            "create_be" => {
                /*
                 * Create root pool:
                 */
                let rootds = format!("{}/ROOT", ib.temp_pool());
                dataset_create(log, &rootds, false)?;
                zfs_set(log, &rootds, "canmount", "off")?;
                zfs_set(log, &rootds, "mountpoint", "legacy")?;

                /*
                 * Create a BE of sorts:
                 */
                let beds = format!("{}/{}", rootds, ib.bename());
                dataset_create(log, &beds, false)?;
                zfs_set(log, &beds, "canmount", "noauto")?;
                zfs_set(log, &beds, "mountpoint", "legacy")?;

                /*
                 * Mount that BE:
                 */
                let targmp = ib.root()?;
                ensure::directory(log, &targmp, ROOT, ROOT, 0o755)?;
                ensure::run(log, &["/sbin/mount", "-F", "zfs", &beds,
                    targmp.to_str().unwrap()])?;

                /*
                 * Set some BE properties...
                 */
                let uuid = Uuid::new_v4().to_hyphenated().to_string();
                info!(log, "boot environment UUID: {}", uuid);
                zfs_set(log, &beds, "org.opensolaris.libbe:uuid", &uuid)?;
                zfs_set(log, &beds, "org.opensolaris.libbe:policy", "static")?;
            }
            "create_dataset" => {
                #[derive(Deserialize)]
                struct CreateDatasetArgs {
                    name: String,
                    mountpoint: Option<String>,
                }

                let a: CreateDatasetArgs = step.args()?;
                let ds = format!("{}/{}", ib.temp_pool(), a.name);
                dataset_create(log, &ds, false)?;
                if let Some(mp) = &a.mountpoint {
                    zfs_set(log, &ds, "mountpoint", mp)?;
                }
            }
            "remove_files" => {
                #[derive(Deserialize)]
                struct RemoveFilesArgs {
                    file: Option<PathBuf>,
                    dir: Option<PathBuf>,
                    pattern: Option<String>,
                }

                let a: RemoveFilesArgs = step.args()?;

                match (&a.file, &a.dir, &a.pattern) {
                    (Some(f), None, None) => {
                        if !f.is_absolute() {
                            bail!("file should be an absolute path");
                        }

                        let mut actual = ib.root()?;
                        actual.extend(f.components().skip(1));
                        info!(log, "remove file: {:?}", actual);
                        std::fs::remove_file(actual)?;
                    }
                    (None, Some(d), None) => {
                        if !d.is_absolute() {
                            bail!("dir should be an absolute path");
                        }

                        let mut actual = ib.root()?;
                        actual.extend(d.components().skip(1));
                        info!(log, "remove tree: {:?}", actual);
                        std::fs::remove_dir_all(actual)?;
                    }
                    (None, None, Some(p)) => {
                        let g = glob::Pattern::new(p)?;
                        let mut w = walkdir::WalkDir::new(ib.root()?)
                            .min_depth(1)
                            .follow_links(false)
                            .contents_first(true)
                            .same_file_system(true)
                            .into_iter();
                        while let Some(ent) = w.next().transpose()? {
                            if !ent.file_type().is_file() {
                                continue;
                            }

                            if let Some(s) = ent.file_name().to_str() {
                                if g.matches(s) {
                                    info!(log, "remove file: {:?}", ent.path());
                                    std::fs::remove_file(ent.path())?;
                                }
                                continue;
                            } else {
                                bail!("path {:?} cannot be matched?",
                                    ent.path());
                            }
                        }
                    }
                    _ => {
                        bail!("must specify exactly one of \"file\", \"dir\", \
                            or \"pattern\"");
                    }
                }
            }
            "unpack_tar" => {
                #[derive(Deserialize)]
                struct UnpackTarArgs {
                    name: String,
                    into_tmp: Option<bool>,
                }

                let a: UnpackTarArgs = step.args()?;
                let name = ib.expand(&a.name)?;
                let targdir = if a.into_tmp.unwrap_or(false) {
                    /*
                     * Store unpacked files in a temporary directory so that
                     * "ensure_file" steps can access the unpacked files using
                     * the "tarsrc" source.
                     */
                    let dir = ib.tmp_file("unpack_tar")?;
                    if dir.exists() {
                        std::fs::remove_dir_all(&dir)?;
                    }
                    std::fs::create_dir(&dir)?;
                    dir.to_str().unwrap().to_string()
                } else {
                    let mp = ib.root()?;
                    mp.to_str().unwrap().to_string()
                };

                /*
                 * Unpack a tar file of an image created by another build:
                 */
                let zflag = if name.ends_with("gz") { "z" } else { "" };
                let tarf = ib.output_file(&name)?;
                ensure::run(log,
                    &["/usr/sbin/tar", &format!("x{zflag}eEp@/f"),
                        tarf.to_str().unwrap(),
                        "-C", &targdir])?;
            }
            "pack_tar" => {
                #[derive(Deserialize)]
                struct PackTarArgs {
                    name: String,
                    include: Option<Vec<String>>,
                }

                let a: PackTarArgs = step.args()?;
                let name = ib.expand(&a.name)?;
                let mp = ib.root()?;

                /*
                 * Create a tar file of the contents of the IPS image that we
                 * can subsequently unpack into ZFS pools or UFS file systems.
                 */
                let tarf = ib.output_file(&name)?;
                ensure::removed(log, &tarf)?;

                let zflag = if name.ends_with("gz") { "z" } else { "" };
                let flags = format!("c{zflag}eEp@/f");
                let mut args = vec!["/usr/sbin/tar", &flags,
                    tarf.to_str().unwrap()];
                if let Some(include) = &a.include {
                    include.iter().for_each(|s| {
                        args.push("-C");
                        args.push(mp.to_str().unwrap());
                        args.push(s.as_str());
                    });
                } else {
                    args.push("-C");
                    args.push(mp.to_str().unwrap());
                    args.push(".");
                }
                ensure::run(log, &args)?;
            }
            "onu" => {
                #[derive(Deserialize)]
                struct OnuArgs {
                    repo: String,
                    publisher: String,
                    #[serde(default)]
                    uninstall: Vec<String>,
                }

                let a: OnuArgs = step.args()?;
                let repo = ib.expand(&a.repo)?;
                let publisher = ib.expand(&a.publisher)?;
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                /*
                 * Upgrade to onu bits:
                 */
                let publ = "on-nightly";
                pkg(log, &["-R", targmp, "set-publisher",
                    "--no-refresh",
                    "--non-sticky",
                    &publisher,
                ])?;
                pkg(log, &["-R", targmp, "set-publisher",
                    "-e",
                    "--no-refresh",
                    "-P",
                    "-O", &repo,
                    publ,
                ])?;
                pkg(log, &["-R", targmp, "refresh", "--full"])?;
                if !a.uninstall.is_empty() {
                    let mut args = vec!["-R", targmp, "uninstall"];
                    for pkg in a.uninstall.iter() {
                        args.push(pkg.as_str());
                    }
                    pkg(log, &args)?;
                }
                pkg(log, &["-R", targmp, "change-facet",
                    "onu.ooceonly=false"
                ])?;
                pkg(log, &["-R", targmp, "update"])?;
                pkg(log, &["-R", targmp, "purge-history"])?;
            }
            "devfsadm" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                /*
                 * Create expected /dev structure.  Note that this can leak some
                 * amount of device information from the live system into the
                 * image; templates should clean up any unexpected links or
                 * nodes.
                 */
                ensure::run(log, &["/usr/sbin/devfsadm", "-r", targmp])?;
            }
            "assemble_files" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                /*
                 * This step mimics assemble_files() in the OmniOS bootadm.
                 */
                #[derive(Deserialize)]
                struct AssembleFileArgs {
                    dir: String,
                    output: String,
                    prefix: Option<String>,
                }

                let a: AssembleFileArgs = step.args()?;

                if !a.dir.starts_with('/') || !a.output.starts_with('/') {
                    bail!("dir and output must be fully qualified");
                }

                let indir = format!("{}{}", targmp, a.dir);
                let outfile = format!("{}{}", targmp, a.output);

                let mut files: Vec<String> = Vec::new();
                let mut diri = std::fs::read_dir(&indir)?;
                while let Some(ent) = diri.next().transpose()? {
                    if !ent.file_type().unwrap().is_file() {
                        continue;
                    }

                    let n = ent.file_name();
                    let n = n.to_str().unwrap();
                    if let Some(prefix) = a.prefix.as_deref() {
                        if !n.starts_with(prefix) {
                            continue;
                        }
                    }

                    files.push(ent.path().to_str().unwrap().to_string());
                }

                files.sort();

                let mut outstr = String::new();
                for f in files.iter() {
                    let inf = std::fs::read_to_string(&f)?;
                    let out = inf.trim();
                    if out.is_empty() {
                        continue;
                    }
                    outstr += out;
                    if !outstr.ends_with('\n') {
                        outstr += "\n";
                    }
                }

                ensure::filestr(log, &outstr, &outfile, ROOT, ROOT, 0o644,
                    Create::Always)?;
            }
            "shadow" => {
                #[derive(Deserialize)]
                struct ShadowArgs {
                    username: String,
                    password: Option<String>,
                }

                let a: ShadowArgs = step.args()?;

                /*
                 * Read the shadow file:
                 */
                let mut path = ib.root()?;
                path.push("etc");
                path.push("shadow");

                let orig = ShadowFile::load(&path)?;
                let mut copy = orig.clone();

                if let Some(password) = a.password.as_deref() {
                    copy.password_set(&a.username, password)?;
                }

                if orig == copy {
                    info!(log, "no change to shadow file; skipping write");
                } else {
                    info!(log, "updating shadow file");
                    copy.write(&path)?;
                    ensure::perms(&ib.log, &path, ROOT, ROOT, 0o400)?;
                }
            }
            "gzip" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct DigestArgs {
                    target: String,
                    src: String,
                    owner: String,
                    group: String,
                    mode: String,
                }

                let a: DigestArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;
                let target = ib.expand(&a.target)?;
                let src = ib.expand(&a.src)?;

                if !target.starts_with('/') {
                    bail!("target must be fully qualified path");
                }
                let target = format!("{}{}", targmp, target);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                if src.starts_with('/') {
                    bail!("source file must be a relative path");
                }
                let src = ib.output_file(&src)?;

                gzip(log, &src, &target)?;
                ensure::perms(log, &target, owner, group, mode)?;
            }
            "digest" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct DigestArgs {
                    algorithm: String,
                    target: String,
                    src: String,
                    owner: String,
                    group: String,
                    mode: String,
                }

                let a: DigestArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;
                let target = ib.expand(&a.target)?;
                let src = ib.expand(&a.src)?;

                let ht = match a.algorithm.as_str() {
                    "sha1" => HashType::SHA1,
                    "md5" => HashType::MD5,
                    x => bail!("unknown digest algorithm {}", x),
                };

                if !target.starts_with('/') {
                    bail!("target must be fully qualified path");
                }
                let target = format!("{}{}", targmp, target);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                if src.starts_with('/') {
                    bail!("source file must be a relative path");
                }
                let src = ib.output_file(&src)?;

                let mut hash = ensure::hash_file(&src, &ht)?;
                hash += "\n";

                ensure::filestr(log, &hash, &target, owner, group, mode,
                    Create::Always)?;
            }
            "ensure_symlink" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct SymlinkArgs {
                    link: String,
                    target: String,
                    owner: String,
                    group: String,
                }

                let a: SymlinkArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;

                if !a.link.starts_with('/') {
                    bail!("link must be fully qualified path");
                }
                let link = format!("{}{}", targmp, a.link);

                ensure::symlink(log, &link, &a.target, owner, group)?;
            }
            "ensure_perms" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct PermsArgs {
                    path: String,
                    owner: String,
                    group: String,
                    mode: String,
                }

                let a: PermsArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;

                if !a.path.starts_with('/') {
                    bail!("path must be fully qualified path");
                }
                let path = format!("{}{}", targmp, a.path);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                ensure::perms(log, &path, owner, group, mode)?;
            }
            "ensure_directory" | "ensure_dir" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct DirArgs {
                    dir: String,
                    owner: String,
                    group: String,
                    mode: String,
                }

                let a: DirArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;

                if !a.dir.starts_with('/') {
                    bail!("dir must be fully qualified path");
                }
                let dir = format!("{}{}", targmp, a.dir);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                ensure::directory(log, &dir, owner, group, mode)?;
            }
            "ensure_file" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct FileArgs {
                    src: Option<String>,
                    imagesrc: Option<String>,
                    tarsrc: Option<String>,
                    outputsrc: Option<String>,
                    extsrc: Option<String>,
                    contents: Option<String>,
                    file: String,
                    owner: String,
                    group: String,
                    mode: String,
                }

                let a: FileArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;
                let src = ib.expando(a.src.as_deref())?;
                let imagesrc = ib.expando(a.imagesrc.as_deref())?;
                let tarsrc = ib.expando(a.tarsrc.as_deref())?;
                let outputsrc = ib.expando(a.outputsrc.as_deref())?;
                let extsrc = ib.expando(a.extsrc.as_deref())?;
                let file = ib.expand(&a.file)?;

                if !file.starts_with('/') {
                    bail!("file must be fully qualified path");
                }
                let file = format!("{}{}", targmp, file);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                if let Some(src) = &src {
                    /*
                     * "src" specifies a source file from within the template
                     * directory structure, whether at the top level or at the
                     * group-specific level.
                     */
                    if src.starts_with('/') {
                        bail!("source file must be a relative path");
                    }
                    let src = ib.template_file(src)?;
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(extsrc) = &extsrc {
                    /*
                     * "extsrc" specifies a source file that comes from one or
                     * more proto area-like trees of files passed in via the -E
                     * option.
                     */
                    if extsrc.starts_with('/') {
                        bail!("external source file must be a relative path");
                    }
                    let src = ib.external_src_file(extsrc)?;
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(outputsrc) = &outputsrc {
                    /*
                     * "outputsrc" specifies a source file from within the
                     * output area.  Useful for including the output of a
                     * previous build (e.g., an EFI system partition) as a file
                     * in the target image (e.g., a bootable ISO).
                     */
                    if outputsrc.starts_with('/') {
                        bail!("output source file must be a relative path");
                    }
                    let src = ib.output_file(outputsrc)?;
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(imagesrc) = &imagesrc {
                    /*
                     * "imagesrc" specifies a source file that already exists
                     * within the image.  Can be used to make a copy of an
                     * existing file at another location.
                     */
                    if !imagesrc.starts_with('/') {
                        bail!("image source file must be fully qualified");
                    }
                    let src = format!("{}{}", targmp, imagesrc);
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(tarsrc) = &tarsrc {
                    /*
                     * "tarsrc" specifies a file in the temporary directory
                     * created by the last "unpack_tar" action that used
                     * "into_tmp".  This can be used to unpack a tar file and
                     * then copy select files from inside that archive to new
                     * names and paths within the target image.
                     */
                    if !tarsrc.starts_with('/') {
                        bail!("tmp tar source file must be fully qualified");
                    }
                    let tardir = ib.tmp_file("unpack_tar")?;
                    let src = format!("{}{}", tardir.to_str().unwrap(), tarsrc);
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(contents) = &a.contents {
                    /*
                     * "contents" provides a literal string in the template to
                     * construct the target file.
                     */
                    ensure::filestr(log, contents, &file, owner, group,
                        mode, Create::Always)?;
                } else {
                    bail!("must specify either \"src\" or \"contents\"");
                }
            }
            "make_bootable" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                let rootds = format!("{}/ROOT", ib.temp_pool());
                let beds = format!("{}/{}", rootds, ib.bename());
                zpool_set(log, &ib.temp_pool(), "bootfs", &beds)?;

                ensure::run(log, &["/sbin/beadm", "activate", ib.bename()])?;
                ensure::run(log, &["/sbin/bootadm", "install-bootloader",
                    "-M", "-f", "-P", &ib.temp_pool(), "-R", targmp])?;
                ensure::run(log, &["/sbin/bootadm", "update-archive",
                    "-f", "-R", targmp])?;
            }
            "pkg_image_create" => {
                #[derive(Deserialize)]
                struct PkgImageCreateArgs {
                    publisher: Option<String>,
                    uri: Option<String>,
                }

                let a: PkgImageCreateArgs = step.args()?;
                let publisher = ib.expando(a.publisher.as_deref())?;
                let uri = ib.expando(a.uri.as_deref())?;
                let mp = ib.root()?;

                if publisher.is_some() != uri.is_some() {
                    bail!("publisher and uri must be specified together");
                }

                let mut args = vec!["image-create", "--full"];
                let pubarg;
                if publisher.is_some() {
                    pubarg = format!("{}={}", publisher.unwrap(), uri.unwrap());
                    args.push("--publisher");
                    args.push(&pubarg);
                }
                args.push(mp.to_str().unwrap());

                pkg(log, &args)?;
            }
            "pkg_install" => {
                #[derive(Deserialize)]
                struct PkgInstallArgs {
                    pkgs: Vec<String>,
                    #[serde(default)]
                    include_optional: bool,
                    strip_optional_publishers: Option<bool>,
                }

                let a: PkgInstallArgs = step.args()?;
                let mp = ib.root()?;

                let pkgs_expanded = a
                    .pkgs
                    .iter()
                    .map(|s| ib.expand(&s))
                    .collect::<Result<Vec<_>>>()?;

                pkg_install(log, mp.to_str().unwrap(), &pkgs_expanded)?;

                if a.include_optional {
                    let mut pkgs = Vec::new();

                    /*
                     * By default it seems that IPS ignores the publisher in an
                     * FMRI for a require dependency, and we should also.
                     */
                    let strip_publishers = a.strip_optional_publishers
                        .unwrap_or(true);

                    /*
                     * For each package, expand any optional dependencies and
                     * add those to the install list.
                     *
                     * XXX It's possible we should look at the
                     * "opensolaris.zone" variant here; for now we assume we are
                     * in the global zone and all packages are OK.
                     */
                    for pkg in pkgs_expanded {
                        let opts = pkg_optional_deps(log,
                            mp.to_str().unwrap(),
                            pkg.as_str(),
                            strip_publishers)?;

                        for opt in opts {
                            if pkgs.contains(&opt) {
                                continue;
                            }

                            info!(log, "optional package: {} -> {}", pkg, opt);
                            pkgs.push(opt);
                        }
                    }

                    if !pkgs.is_empty() {
                        let pkgs: Vec<_> = pkgs.iter().map(|s| s.as_str())
                            .collect();
                        pkg_install(log,
                            mp.to_str().unwrap(),
                            pkgs.as_slice())?;
                    }
                }
            }
            "pkg_set_property" => {
                #[derive(Deserialize)]
                struct PkgSetPropertyArgs {
                    name: String,
                    value: String,
                }

                let a: PkgSetPropertyArgs = step.args()?;
                let mp = ib.root()?;

                pkg(log, &["-R", mp.to_str().unwrap(), "set-property",
                    &a.name,
                    &a.value,
                ])?;
            }
            "pkg_set_publisher" => {
                #[derive(Deserialize)]
                struct PkgSetPublisherArgs {
                    publisher: String,
                    uri: String,
                }

                let a: PkgSetPublisherArgs = step.args()?;
                let mp = ib.root()?;
                let publisher = ib.expand(&a.publisher)?;
                let uri = ib.expand(&a.uri)?;

                pkg(log, &["-R", mp.to_str().unwrap(), "set-publisher",
                    "--no-refresh",
                    "-O", &uri,
                    &publisher,
                ])?;
            }
            "pkg_approve_ca_cert" => {
                #[derive(Deserialize)]
                struct PkgApproveCaCertArgs {
                    publisher: String,
                    certfile: String,
                }

                let a: PkgApproveCaCertArgs = step.args()?;
                let mp = ib.root()?;

                /*
                 * The certificate file to use is in the templates area.
                 */
                if a.certfile.starts_with('/') {
                    bail!("certificate file must be a relative path");
                }
                let cacert = ib.template_file(&a.certfile)?
                    .to_str().unwrap().to_string();

                pkg(log, &["-R", mp.to_str().unwrap(), "set-publisher",
                    &format!("--approve-ca-cert={}", &cacert),
                    &a.publisher,
                ])?;
            }
            "pkg_uninstall" => {
                #[derive(Deserialize)]
                struct PkgUninstallArgs {
                    pkgs: Vec<String>,
                }

                let a: PkgUninstallArgs = step.args()?;
                let mp = ib.root()?;
                let pkgs: Vec<_> = a.pkgs.iter().map(|s| s.as_str()).collect();
                pkg_uninstall(log, mp.to_str().unwrap(), pkgs.as_slice())?;
            }
            "pkg_change_variant" => {
                #[derive(Deserialize)]
                struct PkgChangeVariantArgs {
                    variant: String,
                    value: String,
                }

                let a: PkgChangeVariantArgs = step.args()?;
                let mp = ib.root()?;
                pkg_ensure_variant(log, mp.to_str().unwrap(),
                    &a.variant, &a.value)?;
            }
            "pkg_change_facet" => {
                #[derive(Deserialize)]
                struct PkgChangeFacetArgs {
                    facet: String,
                    value: String,
                }

                let a: PkgChangeFacetArgs = step.args()?;
                let mp = ib.root()?;
                pkg_ensure_facet(log, mp.to_str().unwrap(),
                    &a.facet, &a.value)?;
            }
            "pkg_purge_history" => {
                let mp = ib.root()?;
                pkg(log, &["-R", mp.to_str().unwrap(), "purge-history"])?;
            }
            "seed_smf" => {
                #[derive(Deserialize)]
                struct SeedSmfArgs {
                    debug: Option<bool>,
                    apply_site: Option<bool>,
                    seed: Option<String>,
                    skip_seed: Option<bool>,
                }

                let a: SeedSmfArgs = step.args()?;
                let debug = a.debug.unwrap_or(false);
                let apply_site = a.apply_site.unwrap_or(false);
                let seed = match a.skip_seed.unwrap_or(false) {
                    true => None,
                    false => Some(a.seed.as_deref().unwrap_or("global")),
                };

                seed_smf(log, &ib.svccfg, &ib.tmpdir()?, &ib.root()?, debug,
                    apply_site, seed)?;
            }
            x => {
                bail!("INVALID STEP TYPE: {}", x);
            }
        }

        info!(log, "STEP {} ({}) COMPLETE\n", count, step.t);
    }

    Ok(())
}
