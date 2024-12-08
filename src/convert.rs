use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;

pub(crate) fn convert(input: PathBuf, output: PathBuf, sort: bool) -> anyhow::Result<()> {
    let reader: DataFrameReader = match input.extension() {
        Some(in_ext) => {
            if in_ext == "csv" {
                csv_reader()
            } else if in_ext == "parquet" {
                parquet_reader()
            } else {
                panic!("Unsupported input file extension");
            }
        }
        _ => {
            panic!("Unsupported input file extension");
        }
    };

    let writer: DataFrameWriter = match output.extension() {
        Some(in_ext) => {
            if in_ext == "csv" {
                csv_writer()
            } else if in_ext == "parquet" {
                parquet_writer()
            } else {
                panic!("Unsupported output file extension");
            }
        }
        _ => {
            panic!("Unsupported output file extension");
        }
    };

    let df = reader(input.clone())?;

    let mut df = if sort {
        df.sort(df.get_column_names_str(), SortMultipleOptions::default())?
    } else {
        df
    };

    writer(&mut df, output.clone())?;

    Ok(())
}

type DataFrameReader = Box<dyn FnOnce(PathBuf) -> anyhow::Result<DataFrame>>;

type DataFrameWriter = Box<dyn FnOnce(&mut DataFrame, PathBuf) -> anyhow::Result<()>>;

fn csv_reader() -> DataFrameReader {
    Box::new(|path: PathBuf| {
        let df = CsvReadOptions::default()
            .with_infer_schema_length(None)
            .try_into_reader_with_file_path(Some(path))?
            .finish()?;

        Ok(df)
    })
}

fn csv_writer() -> DataFrameWriter {
    Box::new(|df: &mut DataFrame, path: PathBuf| {
        let mut file = File::create(path)?;
        CsvWriter::new(&mut file).finish(df)?;
        Ok(())
    })
}

fn parquet_reader() -> DataFrameReader {
    Box::new(|path: PathBuf| {
        let mut file = File::open(path)?;
        let df = ParquetReader::new(&mut file).finish()?;
        Ok(df)
    })
}

fn parquet_writer() -> DataFrameWriter {
    Box::new(|df: &mut DataFrame, path: PathBuf| {
        let mut file = File::create(path)?;
        ParquetWriter::new(&mut file)
            .with_compression(ParquetCompression::Zstd(Some(
                ZstdLevel::try_new(22).unwrap(/* zstd max level */),
            )))
            .finish(df)?;
        Ok(())
    })
}
