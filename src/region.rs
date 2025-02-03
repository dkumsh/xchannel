use memmap2::{Mmap, MmapMut, MmapOptions};
use std::fs::File;
use std::{io, io::ErrorKind, slice};

pub type ReadOnly = Mmap;
pub type Writable = MmapMut;

/// Return the OS page size.
#[inline]
pub fn page_size() -> usize {
    // Using the `page_size` crate would be another option, but this avoids a dependency
    // if you prefer. If you want the crate, replace with `page_size::get()`.
    // SAFETY: libc calls are safe here; fall back to 4096 if it fails.
    #[cfg(unix)]
    {
        let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if ps > 0 { ps as usize } else { 4096 }
    }
    #[cfg(windows)]
    {
        // Windows: GetSystemInfo
        use std::mem::MaybeUninit;
        #[repr(C)]
        struct SYSTEM_INFO {
            w: [usize; 16],
        } // avoid binding; we only read dwPageSize at [1]
        extern "system" {
            fn GetSystemInfo(lpSystemInfo: *mut SYSTEM_INFO);
        }
        let mut si = MaybeUninit::<SYSTEM_INFO>::uninit();
        unsafe { GetSystemInfo(si.as_mut_ptr()) };
        let si = unsafe { si.assume_init() };
        // On Windows, page size is USHORT in struct; practical values 4K/8K/64K.
        // Use a conservative default if something odd happens.
        let ps = si.w[1];
        if ps != 0 { ps } else { 4096 }
    }
}

/// Encapsulates a single mmap-ed region.
/// `Mode` is `Mmap` (read-only) or `MmapMut` (writable).
pub struct RegionMapping<Mode> {
    mmap: Mode,
    /// Region offset from the beginning of the file.
    region_offset: u64,
    /// Region size (multiple of OS page size).
    region_size: usize,
}

impl<Mode> RegionMapping<Mode> {
    #[inline]
    pub fn region_offset(&self) -> u64 {
        self.region_offset
    }
    #[inline]
    pub fn region_size(&self) -> usize {
        self.region_size
    }
}

fn check_alignment(base_offset: u64, region_size: usize) -> io::Result<()> {
    let ps = page_size();
    if region_size == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "region_size must be > 0",
        ));
    }
    if !region_size.is_multiple_of(ps) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "region_size ({}) must be a multiple of OS page size ({})",
                region_size, ps
            ),
        ));
    }
    if !base_offset.is_multiple_of(ps as u64) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "base_offset ({}) must be page-aligned (page={})",
                base_offset, ps
            ),
        ));
    }
    Ok(())
}

impl RegionMapping<Writable> {
    pub fn create_writable(file: &File, base_offset: u64, region_size: usize) -> io::Result<Self> {
        check_alignment(base_offset, region_size)?;
        let required_len = base_offset + region_size as u64;
        if file.metadata()?.len() < required_len {
            file.set_len(required_len)?;
        }
        let mmap = unsafe {
            MmapOptions::new()
                .offset(base_offset)
                .len(region_size)
                .map_mut(file)?
        };
        Ok(Self {
            mmap,
            region_offset: base_offset,
            region_size,
        })
    }

    pub fn get_bytes_mut(&mut self, offset: usize, len: usize) -> Option<&mut [u8]> {
        if offset.checked_add(len)? > self.region_size {
            return None;
        }
        let ptr = self.mmap.as_mut_ptr().wrapping_add(offset);
        Some(unsafe { slice::from_raw_parts_mut(ptr, len) })
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.mmap.as_mut_ptr()
    }
}

impl RegionMapping<ReadOnly> {
    pub fn create_read_only(file: &File, base_offset: u64, region_size: usize) -> io::Result<Self> {
        check_alignment(base_offset, region_size)?;
        let mmap = unsafe {
            MmapOptions::new()
                .offset(base_offset)
                .len(region_size)
                .map(file)?
        };
        Ok(Self {
            mmap,
            region_offset: base_offset,
            region_size,
        })
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.mmap.as_ptr()
    }
}
