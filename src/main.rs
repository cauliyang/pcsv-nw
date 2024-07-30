use anyhow::Result;
use clap::Parser;
use polars::frame::row::Row;
use rayon::prelude::*;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use walkdir::WalkDir;

use log::info;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(version, about = "Summary CSV", long_about = None)]
struct Cli {
    /// path to the folder
    folder: PathBuf,

    /// threads number
    #[arg(short, long, default_value = "2")]
    threads: Option<usize>,

    /// prefix for output files
    #[arg(short, long)]
    prefix: Option<String>,

    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,
}

fn find_csv_paths<P>(folder: P) -> Result<Vec<PathBuf>>
where
    P: AsRef<Path>,
{
    let mut csvs = vec![];
    for entry in WalkDir::new(folder).into_iter().filter_map(|e| e.ok()) {
        if entry.path().extension().and_then(|e| e.to_str()) == Some("csv") {
            csvs.push(entry.path().to_path_buf());
        }
    }
    Ok(csvs)
}

fn load_polars_from_path<P: AsRef<Path>>(path: P) -> Result<(PathBuf, DataFrame)> {
    let path_buf = path.as_ref().to_path_buf();
    let df = CsvReadOptions::default()
        .with_has_header(false)
        .try_into_reader_with_file_path(Some(path_buf.clone()))?
        .finish()?;
    Ok((path_buf, df))
}

fn find_minimum_value_in_third_column(df: &DataFrame) -> Result<(usize, Row)> {
    let cl3 = df.column("column_3")?;
    let arg_min = cl3.arg_min().unwrap();
    let _arg_min_value = cl3.get(arg_min)?;
    let min_row = df.get_row(arg_min)?;
    Ok((arg_min, min_row))
}

fn write_result(results: &[(String, usize, Row)]) -> Result<()> {
    let mut writer = csv::Writer::from_writer(io::stdout());
    let mut row_value: Vec<String> = Vec::new();
    for (file_name, _idx, row) in results {
        row_value.clear();
        row_value.push(file_name.clone());

        let row_str = row
            .0
            .par_iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();

        row_value.extend(row_str);
        writer.write_record(&row_value)?;
    }

    Ok(())
}

fn worker<P>(folder: P) -> Result<()>
where
    P: AsRef<Path>,
{
    info!("collect csv paths from folder: {:?}", folder.as_ref());

    let csv_paths = find_csv_paths(folder)?;

    let csv_dfs = csv_paths
        .par_iter()
        .map(load_polars_from_path)
        .collect::<Result<Vec<(PathBuf, DataFrame)>>>()?;

    info!("found {} csv files", csv_dfs.len());

    let results = csv_dfs
        .par_iter()
        .map(|(path, df)| {
            let (row_index, row) = find_minimum_value_in_third_column(df)?;
            Ok((
                path.file_stem().unwrap().to_string_lossy().to_string(),
                row_index,
                row,
            ))
        })
        .collect::<Result<Vec<(String, usize, Row)>>>()?;

    write_result(&results)?;
    Ok(())
}

fn main() -> Result<()> {
    let start = std::time::Instant::now();
    let cli = Cli::parse();

    let log_level = match cli.debug {
        0 => log::LevelFilter::Info,
        1 => log::LevelFilter::Debug,
        2 => log::LevelFilter::Trace,
        _ => log::LevelFilter::Trace,
    };
    // set log level
    env_logger::builder().filter_level(log_level).init();

    rayon::ThreadPoolBuilder::new()
        .num_threads(cli.threads.unwrap())
        .build_global()
        .unwrap();

    info!("threads number: {}", cli.threads.unwrap());

    worker(cli.folder)?;

    let elapsed = start.elapsed();
    log::info!("elapsed time: {:.2?}", elapsed);
    Ok(())
}
