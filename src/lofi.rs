/*
 * Copyright 2021 Oxide Computer Company
 */

use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};
use std::os::unix::ffi::OsStrExt;
use anyhow::{Result, Context, bail};

#[allow(dead_code)]
fn errno() -> i32 {
    unsafe {
        let enp = libc::___errno();
        *enp
    }
}

fn clear_errno() {
    unsafe {
        let enp = libc::___errno();
        *enp = 0;
    }
}

#[repr(i32)]
#[allow(non_camel_case_types)]
#[allow(dead_code)]
enum IVMethod {
    IVM_NONE = 0,
    IVM_ENC_BLKNO,
}

#[repr(i32)]
#[allow(non_camel_case_types)]
#[derive(PartialEq)]
enum Boolean {
    B_FALSE = 0,
    B_TRUE = 1,
}

impl Boolean {
    fn new(val: bool) -> Boolean {
        if val {
            Boolean::B_TRUE
        } else {
            Boolean::B_FALSE
        }
    }
}

#[repr(C)]
struct LofiIoctl {
    li_id: u32,
    li_force: Boolean,
    li_cleanup: Boolean,
    li_readonly: Boolean,
    li_labeled: Boolean,
    li_filename: [libc::c_char; MAXPATHLEN],
    li_devpath: [libc::c_char; MAXPATHLEN],
    li_algorithm: [libc::c_char; MAXALGLEN],
    li_crypto_enabled: Boolean,
    li_cipher: [libc::c_char; CRYPTO_MAX_MECH_NAME],
    li_key_len: u32,
    li_key: [libc::c_char; 56],
    li_iv_cipher: [libc::c_char; CRYPTO_MAX_MECH_NAME],
    li_iv_len: u32,
    li_iv_type: IVMethod,
}

/*
 * XXX These ioctls are all private and thus should not be used here.  Once a
 * public liblofiadm library becomes available, we will switch to using that
 * instead.
 */
const LOFI_IOC_BASE: i32 = ((b'L' as i32) << 16) | ((b'F' as i32) << 8);
const LOFI_MAP_FILE: i32 = LOFI_IOC_BASE | 0x01;
const LOFI_UNMAP_FILE_MINOR: i32 = LOFI_IOC_BASE | 0x04;
const LOFI_GET_FILENAME: i32 = LOFI_IOC_BASE | 0x05;
#[allow(dead_code)]
const LOFI_GET_MINOR: i32 = LOFI_IOC_BASE | 0x06;
const LOFI_GET_MAXMINOR: i32 = LOFI_IOC_BASE | 0x07;

const MAXPATHLEN: usize = 1024;
const MAXALGLEN: usize = 36;
const CRYPTO_MAX_MECH_NAME: usize = 32;

#[derive(Debug, PartialEq)]
pub struct LofiDevice {
    pub id: u32,
    pub readonly: bool,
    pub labeled: bool,
    pub encrypted: bool,
    pub filename: PathBuf,
    pub devpath: Option<PathBuf>,
    pub rdevpath: Option<PathBuf>,
}

impl LofiDevice {
    fn from_ioctl(li: &LofiIoctl) -> LofiDevice {
        let labeled = li.li_labeled != Boolean::B_FALSE;

        let (devpath, rdevpath) = if li.li_devpath[0] == 0 {
            if labeled {
                /*
                 * Devices with a label do not have a predictable path.
                 */
                (None, None)
            } else {
                (
                    Some(format!("/dev/lofi/{}", li.li_id)),
                    Some(format!("/dev/rlofi/{}", li.li_id)),
                )
            }
        } else {
            let mut rdevpath = unsafe { CStr::from_ptr(li.li_devpath.as_ptr()) }
                .to_str()
                .unwrap()
                .to_string();

            if labeled {
                rdevpath.push_str("p0");
            }

            let devpath = rdevpath.replacen('r', "", 1);

            (Some(devpath), Some(rdevpath))
        };
        let devpath = devpath.map(PathBuf::from);
        let rdevpath = rdevpath.map(PathBuf::from);

        let filename = PathBuf::from(unsafe {
            CStr::from_ptr(li.li_filename.as_ptr())
        }.to_str().unwrap());

        LofiDevice {
            id: li.li_id,
            readonly: li.li_readonly != Boolean::B_FALSE,
            labeled,
            encrypted: li.li_crypto_enabled != Boolean::B_FALSE,
            filename,
            devpath,
            rdevpath,
        }
    }
}

struct LofiFd {
    fd: i32,
}

impl LofiFd {
    fn fd(&self) -> i32 {
        self.fd
    }

    fn ioctl(&self, cmd: i32, li: &mut LofiIoctl) -> Result<u32> {
        clear_errno();
        let res = unsafe { libc::ioctl(self.fd, cmd, li as *mut LofiIoctl) };
        if res < 0 {
            let err = std::io::Error::last_os_error();
            bail!("lofi ioctl error: {}", err);
        }

        /*
         * Ensure the C strings are terminated:
         */
        li.li_devpath[li.li_devpath.len() - 1] = 0;
        li.li_filename[li.li_filename.len() - 1] = 0;

        Ok(res as u32)
    }

    fn open(readonly: bool) -> Result<LofiFd> {
        let flag = libc::O_EXCL | if readonly {
            libc::O_RDONLY
        } else {
            libc::O_RDWR
        };

        let path = std::ffi::CString::new(b"/dev/lofictl".to_vec())?;

        let fd = unsafe { libc::open(path.as_ptr(), flag) };
        if fd < 0 {
            let err = std::io::Error::last_os_error();
            bail!("could not open lofictl: {}", err);
        }

        Ok(LofiFd { fd })
    }
}

impl Drop for LofiFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}

pub fn lofi_list() -> Result<Vec<LofiDevice>> {
    let mut li: LofiIoctl = unsafe { std::mem::zeroed() };

    let fd = LofiFd::open(true)?;

    li.li_id = 0;
    fd.ioctl(LOFI_GET_MAXMINOR, &mut li).context("LOFI_GET_MAXMINOR")?;

    let mut out = Vec::new();

    let maxminor = li.li_id;
    for i in 1..=maxminor {
        li.li_id = i;
        let res = unsafe { libc::ioctl(fd.fd(), LOFI_GET_FILENAME, &mut li) };
        if res == -1 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error().unwrap() == libc::ENXIO {
                /*
                 * This minor number is not currently mapped.  Skip to the next.
                 */
                continue;
            }
            bail!("could not get device for minor {}: {}", i, err);
        }

        out.push(LofiDevice::from_ioctl(&li));
    }

    Ok(out)
}

pub fn lofi_map<P: AsRef<Path>>(file: P, label: bool)
    -> Result<LofiDevice>
{
    let fd = LofiFd::open(false)?;

    let mut li: LofiIoctl = unsafe { std::mem::zeroed() };
    li.li_readonly = Boolean::B_FALSE;
    li.li_labeled = Boolean::new(label);
    li.li_crypto_enabled = Boolean::B_FALSE;

    let o = file.as_ref().as_os_str().as_bytes();
    if o.len() > li.li_filename.len() - 1 {
        bail!("file name too long for ioctl");
    }
    o.iter().enumerate().for_each(|(i, byt)| li.li_filename[i] = *byt as i8);
    li.li_filename[o.len()] = 0;

    fd.ioctl(LOFI_MAP_FILE, &mut li).context("LOFI_MAP_FILE")?;

    let lofi = LofiDevice::from_ioctl(&li);

    if let Some(devpath) = lofi.devpath.as_ref() {
        wait_for_device(devpath)?;
    }
    if let Some(rdevpath) = lofi.rdevpath.as_ref() {
        wait_for_device(rdevpath)?;
    }

    Ok(lofi)
}

#[link(name = "c")]
extern "C" {
    fn __major(version: i32, devnum: u64) -> u32;
    fn __minor(version: i32, devnum: u64) -> u32;
    fn modctl(cmd: u32, arg1: usize, arg2: usize, arg3: usize) -> i32;
}

const MODGETNAME: u32 = 9;
const MODMAXNAMELEN: usize = 32;

/*
 * XXX this is only true on x86:
 */
const LOFI_CMLB_SHIFT: usize = 6;

fn major_to_driver(major: u32) -> Result<String> {
    let mut driver: [u8; MODMAXNAMELEN] = unsafe { std::mem::zeroed() };

    let major: usize = major as usize;
    let r = unsafe { modctl(MODGETNAME, driver.as_mut_ptr() as usize,
        driver.len(), &major as *const _ as usize) };
    if r != 0 {
        let err = std::io::Error::last_os_error();
        bail!("could not determine major number: {}", err);
    }

    if let Some(nul) = driver.iter().position(|b| *b == 0) {
        Ok(CStr::from_bytes_with_nul(&driver[0..=nul])
            .unwrap()
            .to_str()
            .unwrap()
            .to_string())
    } else {
        bail!("no null termination on driver string?");
    }
}

fn wait_for_device<P: AsRef<Path>>(dev: P) -> Result<()> {
    let p = CString::new(dev.as_ref().as_os_str().as_bytes()).unwrap();

    let mut st: libc::stat = unsafe { std::mem::zeroed() };

    /*
     * XXX It looks like we should be trying something like...
     *
     *  di_devlink_init("lofi", DI_MAKE_LINK)
     *
     * ... here.  Really, liblofiadm should take care of these details.
     */

    for _ in 0..=30 {
        if unsafe { libc::stat(p.as_ptr(), &mut st) } == 0 {
            return Ok(());
        }

        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    bail!("could not stat {:?}", dev.as_ref());
}

fn device_to_minor<P: AsRef<Path>>(dev: P) -> Result<u32> {
    let p = CString::new(dev.as_ref().as_os_str().as_bytes()).unwrap();

    let mut st: libc::stat = unsafe { std::mem::zeroed() };

    if unsafe { libc::stat(p.as_ptr(), &mut st) } != 0 {
        let err = std::io::Error::last_os_error();
        bail!("could not stat path: {:?}: {}", dev.as_ref(), err);
    }

    let major = unsafe { __major(1, st.st_rdev) };
    let minor = unsafe { __minor(1, st.st_rdev) };
    let driver = major_to_driver(major)?;

    if &driver != "lofi" {
        bail!("driver was {driver}, not lofi");
    }

    Ok(minor >> LOFI_CMLB_SHIFT)
}

pub fn lofi_unmap_device<P: AsRef<Path>>(devpath: P) -> Result<()> {
    let minor = device_to_minor(devpath)?;

    let fd = LofiFd::open(false)?;

    let mut li: LofiIoctl = unsafe { std::mem::zeroed() };
    li.li_id = minor;

    fd.ioctl(LOFI_UNMAP_FILE_MINOR, &mut li).context("LOFI_UNMAP_FILE_MINOR")?;

    Ok(())
}
