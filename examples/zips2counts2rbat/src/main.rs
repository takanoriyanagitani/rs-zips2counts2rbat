use std::io;
use std::path::Path;
use std::process::ExitCode;

use rs_zips2counts2rbat::arrow;

use arrow::record_batch::RecordBatch;

use rs_zips2counts2rbat::FsZipStatSource16;
use rs_zips2counts2rbat::dir2batch_fs16;

fn stem2u16(stem: &str) -> Result<u16, io::Error> {
    let dword: u32 = u32::from_str_radix(stem, 16)
        .map_err(|e| format!("invalid file stem {stem}: {e}"))
        .map_err(io::Error::other)?;
    let lo: u32 = dword & 0xffff;
    Ok(lo as u16)
}

fn path2u16(p: &Path) -> Result<u16, io::Error> {
    let stem: &str = p.file_stem().and_then(|o| o.to_str()).unwrap_or_default();
    stem2u16(stem)
}

fn sub() -> Result<(), io::Error> {
    let dirname: String = std::env::var("ENV_DIR_NAME").unwrap_or_default();
    let iname: &str = "id";
    let cname: &str = "count";
    let fzs2 = FsZipStatSource16 { key2id: path2u16 };
    let rb: RecordBatch = dir2batch_fs16(dirname, fzs2, iname, cname)?;

    println!("{rb:#?}");

    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(err) => {
            println!("{err}");
            ExitCode::FAILURE
        }
    }
}
