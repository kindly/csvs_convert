use std::io::{Seek, Write};
use std::iter::Iterator;
use zip::result::ZipError;
use zip::write::{FileOptions, SimpleFileOptions};

use std::fs::File;
use std::path::Path;
use walkdir::{DirEntry, WalkDir};

fn zip_iterator<T>(
    it: &mut dyn Iterator<Item = DirEntry>,
    prefix: &Path,
    writer: T,
) -> zip::result::ZipResult<()>
where
    T: Write + Seek,
{
    let mut zip = zip::ZipWriter::new(writer);
    let options: SimpleFileOptions = FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .unix_permissions(0o755);

    for entry in it {
        let path = entry.path();
        let name = path
            .strip_prefix(Path::new(prefix))
            .map_err(|_| zip::result::ZipError::InvalidArchive("Could not strip prefix".into()))?;

        if path.is_file() {
            zip.start_file(name.to_string_lossy(), options)?;
            let mut f = File::open(path)?;
            std::io::copy(&mut f, &mut zip)?;
        } else if !name.as_os_str().is_empty() {
            zip.add_directory(name.to_string_lossy(), options)?;
        }
    }
    zip.finish()?;
    Result::Ok(())
}

pub fn zip_dir(src_dir: &Path, dst_file: &Path) -> zip::result::ZipResult<()> {
    if !Path::new(src_dir).is_dir() {
        return Err(ZipError::FileNotFound);
    }

    let path = Path::new(dst_file);
    let file = File::create(path)?;

    let walkdir = WalkDir::new(src_dir);
    let it = walkdir.into_iter();

    zip_iterator(&mut it.filter_map(|e| e.ok()), src_dir, file)?;

    Ok(())
}
