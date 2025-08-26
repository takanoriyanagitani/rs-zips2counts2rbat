pub use arrow;

use std::fs::File;
use std::io;
use std::sync::Arc;

use std::path::Path;
use std::path::PathBuf;

use zip::read::ZipArchive;

use arrow::array::PrimitiveArray;
use arrow::record_batch::RecordBatch;

use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;

use arrow::datatypes::UInt16Type;

pub struct PrimitiveZipStat<I, C>
where
    I: ArrowPrimitiveType,
    C: ArrowPrimitiveType,
{
    pub id: I::Native,
    pub count: C::Native,
}

pub trait ZipStatSource {
    type K;

    type I: ArrowPrimitiveType;
    type C: ArrowPrimitiveType;

    fn to_stat(&self, key: &Self::K) -> Result<PrimitiveZipStat<Self::I, Self::C>, io::Error>;
}

pub struct FsZipStatSource16<F> {
    pub key2id: F,
}

impl<F> ZipStatSource for FsZipStatSource16<F>
where
    F: Fn(&Path) -> Result<u16, io::Error>,
{
    type K = PathBuf;

    type I = UInt16Type;
    type C = UInt16Type;

    fn to_stat(&self, key: &Self::K) -> Result<PrimitiveZipStat<Self::I, Self::C>, io::Error> {
        let id: u16 = (self.key2id)(key)?;

        let f: File = File::open(key)?;
        let za = ZipArchive::new(f)?;

        let sz: usize = za.len();
        let i: u16 = sz.try_into().map_err(io::Error::other)?;
        Ok(PrimitiveZipStat { id, count: i })
    }
}

pub fn keys2batch<I, Z>(
    keys: I,
    zsrc: Z,
    iname: &str,
    cname: &str,
) -> Result<RecordBatch, io::Error>
where
    Z: ZipStatSource,
    I: Iterator<Item = Result<Z::K, io::Error>>,
{
    let mut ids: Vec<_> = vec![];
    let mut counts: Vec<_> = vec![];
    for rk in keys {
        let k: Z::K = rk?;
        let pzs: PrimitiveZipStat<_, _> = zsrc.to_stat(&k)?;

        let id = pzs.id;
        let cnt = pzs.count;

        ids.push(id);
        counts.push(cnt);
    }

    let iarr: PrimitiveArray<_> = PrimitiveArray::<<Z as ZipStatSource>::I>::from_iter_values(ids);
    let carr: PrimitiveArray<_> =
        PrimitiveArray::<<Z as ZipStatSource>::C>::from_iter_values(counts);

    let idtyp: DataType = Z::I::DATA_TYPE;
    let cdtyp: DataType = Z::C::DATA_TYPE;

    let sch = Schema::new(vec![
        Field::new(iname, idtyp, false),
        Field::new(cname, cdtyp, false),
    ]);

    RecordBatch::try_new(Arc::new(sch), vec![Arc::new(iarr), Arc::new(carr)])
        .map_err(io::Error::other)
}

pub fn keys2batch_fs16<I, F>(
    keys: I,
    zsrc: FsZipStatSource16<F>,
    iname: &str,
    cname: &str,
) -> Result<RecordBatch, io::Error>
where
    F: Fn(&Path) -> Result<u16, io::Error>,
    I: Iterator<Item = Result<PathBuf, io::Error>>,
{
    keys2batch(keys, zsrc, iname, cname)
}

pub fn dir2batch_fs16<F, P>(
    dir: P,
    zsrc: FsZipStatSource16<F>,
    iname: &str,
    cname: &str,
) -> Result<RecordBatch, io::Error>
where
    F: Fn(&Path) -> Result<u16, io::Error>,
    P: AsRef<Path>,
{
    let dirents = std::fs::read_dir(dir)?;
    let keys = dirents.filter_map(|rdir| match rdir {
        Err(e) => Some(Err(e)),
        Ok(dirent) => {
            let p: PathBuf = dirent.path();
            let is_zip: bool = p.extension().map(|o| o.eq("zip")).unwrap_or_default();
            is_zip.then_some(Ok(p))
        }
    });
    keys2batch_fs16(keys, zsrc, iname, cname)
}
