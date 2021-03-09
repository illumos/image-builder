/*
 * Copyright 2021 Oxide Computer Company
 */

use std::process::{Command, exit};
use anyhow::{Result, Context, bail, anyhow};
use std::collections::HashMap;
use jmclib::log::prelude::*;
use serde::Deserialize;
use std::path::{PathBuf, Path};
use uuid::Uuid;
use std::io::{Read, Write};

mod lofi;
mod ensure;

use ensure::{Create, HashType};

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
            opts.reqopt("d", "dataset", "root dataset for work", "DATASET");
            opts.optopt("T", "templates", "directory for templates", "DIR");
            opts.optopt("S", "svccfg", "svccfg-native location", "SVCCFG");

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

    /*
     * Check to make sure it is not already in lofi:
     */
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
    } else {
        info!(log, "no lofi found");
    }

    ensure::removed(log, &imagefile)?;

    let pool = ib.template.pool.as_ref().unwrap();

    /*
     * Create the file that we will use as the backing store for the pool.  The
     * size of this file is the resultant disk image size.
     */
    mkfile(log, &imagefile, pool.size())?;

    /*
     * Attach this file as a labelled lofi(7D) device so that we can manage
     * slices.
     */
    let lofi = lofi::lofi_map(&imagefile, true)?;
    let ldev = lofi.devpath.as_ref().unwrap();
    info!(log, "lofi device = {}", ldev.display());

    let disk = ldev.to_str().unwrap().trim_end_matches("p0");
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
    let mut args = vec![
        "/sbin/zpool", "create",
        "-d",
        "-t", &temppool,
        "-O", "compression=on",
        "-R", altroot.to_str().unwrap(),
    ];

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

    let targpool = ib.target_pool();
    args.push(&targpool);
    args.push(&disk);

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
    lofi::lofi_unmap_device(&ldev)?;

    /*
     * Copy the image file to the output directory.
     */
    let outputfile = ib.output_file(&format!("{}-{}.raw", ib.group,
        ib.name))?;

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
    let ibrootds = mat.opt_str("d").unwrap();
    let fullreset = mat.opt_present("x");
    let reset = mat.opt_present("r");
    let template_root = find_template_root(mat.opt_str("T"))?;

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

    let t = load_template(&template_root, &group, &name, false)
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

    let tmpds = format!("{}/tmp/{}/{}", ibrootds, group, name);
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
    let bename = Uuid::new_v4().to_hyphenated().to_string()[0..8].to_string();

    if let Some(pool) = &t.pool {
        assert!(t.dataset.is_none());

        let workds = format!("{}/work/{}/{}", ibrootds, group, name);
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

        let mut ib = ImageBuilder {
            build_type: BuildType::Pool(pool.name().to_string()),
            bename,
            group,
            name,
            template_root,
            template: t,
            workds,
            outputds,
            tmpds,
            svccfg,
            log: log.clone(),
        };

        run_build_pool(&mut ib)?;

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
    let input_snapshot = dataset.input_snapshot.as_deref();
    let output_snapshot = dataset.output_snapshot.as_deref();

    let workds = format!("{}/work/{}/{}", ibrootds, group, dataset.name);
    info!(log, "work dataset: {}", workds);

    let mut ib = ImageBuilder {
        build_type: BuildType::Dataset,
        bename,
        group: group.clone(),
        name: name.clone(),
        template_root,
        template: t.clone(),
        workds: workds.clone(),
        outputds,
        tmpds,
        svccfg,
        log: log.clone(),
    };

    if fullreset {
        info!(log, "resetting by removing work dataset: {}", workds);
        dataset_remove(log, &workds)?;
    }

    if dataset_exists(&workds)? {
        /*
         * The dataset exists already.  If this template has configured an
         * output snapshot, we can roll back to it without doing any more work.
         */
        if let Some(snap) = output_snapshot {
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
        if let Some(snap) = &input_snapshot {
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

    if let Some(snap) = &output_snapshot {
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

fn pkg_install(log: &Logger, root: &str, packages: &[&str]) -> Result<()> {
    let mut newargs = vec!["/usr/bin/pkg", "-R", root, "install"];
    for pkg in packages {
        newargs.push(pkg);
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

fn seed_smf(log: &Logger, svccfg: &str, tmpdir: &Path, mountpoint: &Path,
    debug: bool) -> Result<()>
{
    let tmpdir = tmpdir.to_str().unwrap();
    let mountpoint = mountpoint.to_str().unwrap();

    let dtd = format!("{}/usr/share/lib/xml/dtd/service_bundle.dtd.1",
        mountpoint);
    let repo = format!("{}/repo.db", tmpdir);
    let seed = format!("{}/lib/svc/seed/{}.db", mountpoint, "global");
    let manifests = format!("{}/lib/svc/manifest", mountpoint);
    let installto = format!("{}/etc/svc/repository.db", mountpoint);

    ensure::file(log, &seed, &repo, ROOT, ROOT, 0o600, Create::Always)?;

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

enum BuildType {
    Pool(String),
    Dataset,
}

struct ImageBuilder {
    build_type: BuildType,

    group: String,
    name: String,

    log: Logger,
    template_root: PathBuf,
    workds: String,
    outputds: String,
    tmpds: String,
    template: Template,
    bename: String,

    svccfg: String,
}

impl ImageBuilder {
    /**
     * The root directory of the target system image.  This is the work dataset
     * mountpoint in the case of a dataset build, or the /a directory underneath
     * that for a pool build.
     */
    fn root(&self) -> Result<PathBuf> {
        let mut s = zfs_get(&self.workds, "mountpoint")?;

        match &self.build_type {
            /*
             * When a template targets a pool, files target the mountpoint of
             * the boot environment we create in the pool.
             */
            BuildType::Pool(_) => s += "/a",
            BuildType::Dataset => (),
        }

        Ok(PathBuf::from(s))
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
            format!("TEMPORARY-{}-{}", self.group, self.name)
        } else {
            panic!("not a pool job");
        }
    }

    fn tmpdir(&self) -> Result<PathBuf> {
        let s = zfs_get(&self.tmpds, "mountpoint")?;
        Ok(PathBuf::from(s))
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
}

#[derive(Deserialize, Debug, Clone)]
struct Pool {
    name: String,
    ashift: Option<u8>,
    uefi: Option<bool>,
    size: usize,
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

    fn ashift(&self) -> u8 {
        /*
         * Default to 512 byte sectors.
         */
        self.ashift.unwrap_or(9)
    }

    fn size(&self) -> usize {
        self.size
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

fn load_template<P>(root: P, group: &str, name: &str, include: bool)
    -> Result<Template>
    where P: AsRef<Path>
{
    let path = if include {
        include_path(root.as_ref(), group, name)?
    } else {
        let mut path = root.as_ref().to_path_buf();
        path.push(format!("{}/{}.json", group, name));
        path
    };
    let f = std::fs::File::open(&path)
        .with_context(|| anyhow!("template load path: {:?}", &path))?;
    let mut t: Template = serde_json::from_reader(f)?;
    if include {
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
            }

            let a: IncludeArgs = step.args()?;

            let ti = load_template(root.as_ref(), group, &a.name, true)?;
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
                    &targmp.to_str().unwrap()])?;

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
                    zfs_set(log, &ds, "mountpoint", &mp)?;
                }
            }
            "unpack_tar" => {
                #[derive(Deserialize)]
                struct UnpackTarArgs {
                    name: String,
                }

                let a: UnpackTarArgs = step.args()?;
                let mp = ib.root()?;

                /*
                 * Unpack a tar file of an image created by another build:
                 */
                let tarf = ib.output_file(&a.name)?;
                ensure::run(log,
                    &["/usr/sbin/tar", "xzeEp@/f",
                        &tarf.to_str().unwrap(),
                        "-C", &mp.to_str().unwrap()])?;
            }
            "pack_tar" => {
                #[derive(Deserialize)]
                struct PackTarArgs {
                    name: String,
                }

                let a: PackTarArgs = step.args()?;
                let mp = ib.root()?;

                /*
                 * Create a tar file of the contents of the IPS image that we
                 * can subsequently unpack into ZFS pools or UFS file systems.
                 */
                let tarf = ib.output_file(&a.name)?;
                ensure::removed(log, &tarf)?;
                ensure::run(log, &["/usr/sbin/tar", "czeEp@/f",
                    &tarf.to_str().unwrap(),
                    "-C", &mp.to_str().unwrap(),
                    "."])?;
            }
            "onu" => {
                #[derive(Deserialize)]
                struct OnuArgs {
                    repo: String,
                }

                let a: OnuArgs = step.args()?;
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                /*
                 * Upgrade to onu bits:
                 */
                let publ = "on-nightly";
                pkg(log, &["-R", &targmp, "set-publisher",
                    "--no-refresh",
                    "--non-sticky",
                    "openindiana.org",
                ])?;
                pkg(log, &["-R", &targmp, "set-publisher",
                    "-e",
                    "--no-refresh",
                    "-P",
                    "-O", &a.repo,
                    publ,
                ])?;
                pkg(log, &["-R", &targmp, "refresh", "--full"])?;
                pkg(log, &["-R", &targmp, "uninstall",
                    "userland-incorporation",
                    "entire",
                ])?;
                pkg(log, &["-R", &targmp, "update"])?;
                pkg(log, &["-R", &targmp, "purge-history"])?;
            }
            "devfsadm" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                /*
                 * Device file stuff?
                 * XXX Is this really necessary?
                 */
                ensure::run(log, &["/usr/sbin/devfsadm", "-r", &targmp])?;
                /*
                 * XXX Clear out:
                 *  /dev/dsk/
                 *  /dev/rdsk/
                 *  /dev/removable-media/dsk/
                 *  /dev/removable-media/rdsk/
                 *  /dev/cfg/
                 *  /dev/usb/
                 */
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

                let ht = match a.algorithm.as_str() {
                    "sha1" => HashType::SHA1,
                    "md5" => HashType::MD5,
                    x => bail!("unknown digest algorithm {}", x),
                };

                if !a.target.starts_with('/') {
                    bail!("target must be fully qualified path");
                }
                let target = format!("{}{}", targmp, a.target);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                if a.src.starts_with('/') {
                    bail!("source file must be a relative path");
                }
                let src = ib.output_file(&a.src)?;

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

                ensure::symlink(&log, &link, &a.target, owner, group)?;
            }
            "ensure_file" => {
                let mp = ib.root()?;
                let targmp = mp.to_str().unwrap();

                #[derive(Deserialize)]
                struct FileArgs {
                    src: Option<String>,
                    imagesrc: Option<String>,
                    contents: Option<String>,
                    file: String,
                    owner: String,
                    group: String,
                    mode: String,
                }

                let a: FileArgs = step.args()?;
                let owner = translate_uid(&a.owner)?;
                let group = translate_gid(&a.group)?;

                if !a.file.starts_with('/') {
                    bail!("file must be fully qualified path");
                }
                let file = format!("{}{}", targmp, a.file);

                let mode = u32::from_str_radix(&a.mode, 8)?;

                if let Some(src) = &a.src {
                    if src.starts_with('/') {
                        bail!("source file must be a relative path");
                    }
                    let src = ib.template_file(src)?;
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(imagesrc) = &a.imagesrc {
                    if !imagesrc.starts_with('/') {
                        bail!("image source file must be fully qualified");
                    }
                    let src = format!("{}{}", targmp, imagesrc);
                    ensure::file(log, &src, &file, owner, group,
                        mode, Create::Always)?;
                } else if let Some(contents) = &a.contents {
                    ensure::filestr(log, &contents, &file, owner, group,
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
                    "-M", "-f", "-P", &ib.temp_pool(), "-R", &targmp])?;
                ensure::run(log, &["/sbin/bootadm", "update-archive",
                    "-f", "-R", &targmp])?;
            }
            "pkg_image_create" => {
                #[derive(Deserialize)]
                struct PkgImageCreateArgs {
                    publisher: String,
                    uri: String,
                }

                let a: PkgImageCreateArgs = step.args()?;
                let mp = ib.root()?;
                pkg(log, &["image-create", "--full",
                    "--publisher", &format!("{}={}", a.publisher, a.uri),
                    &mp.to_str().unwrap()])?;
            }
            "pkg_install" => {
                #[derive(Deserialize)]
                struct PkgInstallArgs {
                    pkgs: Vec<String>,
                }

                let a: PkgInstallArgs = step.args()?;
                let mp = ib.root()?;
                let pkgs: Vec<_> = a.pkgs.iter().map(|s| s.as_str()).collect();
                pkg_install(log, &mp.to_str().unwrap(), pkgs.as_slice())?;
            }
            "pkg_set_property" => {
                #[derive(Deserialize)]
                struct PkgSetPropertyArgs {
                    name: String,
                    value: String,
                }

                let a: PkgSetPropertyArgs = step.args()?;
                let mp = ib.root()?;

                pkg(log, &["-R", &mp.to_str().unwrap(), "set-property",
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

                pkg(log, &["-R", &mp.to_str().unwrap(), "set-publisher",
                    "--no-refresh",
                    "-O", &a.uri,
                    &a.publisher,
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

                pkg(log, &["-R", &mp.to_str().unwrap(), "set-publisher",
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
                pkg_uninstall(log, &mp.to_str().unwrap(), pkgs.as_slice())?;
            }
            "pkg_change_variant" => {
                #[derive(Deserialize)]
                struct PkgChangeVariantArgs {
                    variant: String,
                    value: String,
                }

                let a: PkgChangeVariantArgs = step.args()?;
                let mp = ib.root()?;
                pkg_ensure_variant(log, &mp.to_str().unwrap(),
                    &a.variant, &a.value)?;
            }
            "pkg_purge_history" => {
                let mp = ib.root()?;
                pkg(log, &["-R", &mp.to_str().unwrap(), "purge-history"])?;
            }
            "seed_smf" => {
                #[derive(Deserialize)]
                struct SeedSmfArgs {
                    debug: Option<bool>,
                }

                let a: SeedSmfArgs = step.args()?;
                let debug = a.debug.unwrap_or(false);

                seed_smf(log, &ib.svccfg, &ib.tmpdir()?, &ib.root()?, debug)?;
            }
            x => {
                bail!("INVALID STEP TYPE: {}", x);
            }
        }

        info!(log, "STEP {} ({}) COMPLETE\n", count, step.t);
    }

    Ok(())
}
