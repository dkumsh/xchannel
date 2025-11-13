use memmap2::{Mmap, MmapMut, MmapOptions};
use std::fs::File;
use std::{io, io::ErrorKind, slice};

pub type ReadOnly = Mmap;
pub type Writable = MmapMut;

/// Return the OS page size.
#[inline]
pub fn page_size() -> usize {
    // SAFETY: libc calls are safe here; fall back to 4096 if it fails.
    #[cfg(unix)]
    {
        let ps = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if ps > 0 { ps as usize } else { 4096 }
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
            let mut opts = MmapOptions::new();
            opts.offset(base_offset).len(region_size);
            // Prefer MAP_POPULATE on Linux to avoid first-touch stalls
            #[cfg(target_os = "linux")]
            let mmap = opts.populate().map_mut(file)?;
            #[cfg(not(target_os = "linux"))]
            let mmap = opts.map_mut(file)?;
            mmap
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
            let mut opts = MmapOptions::new();
            opts.offset(base_offset).len(region_size);
            #[cfg(target_os = "linux")]
            let mmap = opts.populate().map(file)?;
            #[cfg(not(target_os = "linux"))]
            let mmap = opts.map(file)?;
            mmap
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
