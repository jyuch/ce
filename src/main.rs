use clap::Parser;
use polars::prelude::*;
use std::fs::File;
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Cli {
    /// Input.
    #[clap(short, long)]
    input: PathBuf,

    /// Output.
    #[clap(short, long)]
    output: PathBuf,

    /// Sort.
    #[clap(short, long, default_value_t = false)]
    sort: bool,
}

type DataFrameReader = fn(PathBuf) -> anyhow::Result<DataFrame>;

type DataFrameWriter = fn(&mut DataFrame, PathBuf) -> anyhow::Result<()>;

fn main() -> anyhow::Result<()> {
    let opt = Cli::parse();

    let reader: DataFrameReader = match opt.input.extension() {
        Some(in_ext) => {
            if in_ext == "csv" {
                csv_reader
            } else if in_ext == "parquet" {
                parquet_reader
            } else {
                panic!("Unsupported input file extension");
            }
        }
        _ => {
            panic!("Unsupported input file extension");
        }
    };

    let writer: DataFrameWriter = match opt.output.extension() {
        Some(in_ext) => {
            if in_ext == "csv" {
                csv_writer
            } else if in_ext == "parquet" {
                parquet_writer
            } else {
                panic!("Unsupported output file extension");
            }
        }
        _ => {
            panic!("Unsupported output file extension");
        }
    };

    let df = reader(opt.input.clone())?;

    let mut df = if opt.sort {
        df.sort(df.get_column_names_str(), SortMultipleOptions::default())?
    } else {
        df
    };

    writer(&mut df, opt.output.clone())?;

    Ok(())
}

fn csv_reader(path: PathBuf) -> anyhow::Result<DataFrame> {
    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path))?
        .finish()?;

    Ok(df)
}

fn csv_writer(df: &mut DataFrame, path: PathBuf) -> anyhow::Result<()> {
    let mut file = File::create(path)?;
    CsvWriter::new(&mut file).finish(df)?;
    Ok(())
}

fn parquet_reader(path: PathBuf) -> anyhow::Result<DataFrame> {
    let mut file = File::open(path)?;
    let df = ParquetReader::new(&mut file).finish()?;
    Ok(df)
}

fn parquet_writer(df: &mut DataFrame, path: PathBuf) -> anyhow::Result<()> {
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}