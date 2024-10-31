use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use log::debug;
use polars::frame::row::Row;
use rayon::prelude::*;
use std::fmt::Result;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use walkdir::WalkDir;

use log::info;
use polars::prelude::*;

#[derive(Parser, Debug)]
#[command(version, about = "Summary JLD", long_about = None)]
struct Cli {
    /// path to the folder
    folder: PathBuf,

    /// threads number
    #[arg(short, long, default_value = "2")]
    threads: Option<usize>,

    /// maximum number of prcessed files
    #[arg(short, long)]
    max_files: Option<usize>,

    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,
}

fn find_jld_paths<P>(folder: P) -> Result<Vec<PathBuf>>
where
    P: AsRef<Path>,
{
    // find folder starts with "fit"
    let all_folders: Vec<PathBuf> = WalkDir::new(folder.as_ref())
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_str().unwrap().starts_with("fit"))
        .inspect(|e| log::debug!("entry: {:?}", e))
        .map(|e| e.path().to_path_buf())
        .collect();

    log::info!("collect folders: {}", all_folders.len());

    let jlds = all_folders
        .par_iter()
        .flat_map(|folder| {
            let current_jld: Vec<PathBuf> = WalkDir::new(folder)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().and_then(|e| e.to_str()) == Some("jld2"))
                .map(|e| e.path().to_path_buf())
                .collect();
            current_jld
        })
        .collect::<Vec<PathBuf>>();

    Ok(jlds)
}

fn load_polars_from_path<P: AsRef<Path>>(path: P) -> PolarsResult<DataFrame> {
    debug!("load jld file: {:?}", path.as_ref());
    let path_buf = path.as_ref().to_path_buf();

    CsvReadOptions::default()
        .with_has_header(false)
        .try_into_reader_with_file_path(Some(path_buf.clone()))?
        .finish()
}

fn find_minimum_value_in_third_column(df: &DataFrame) -> Result<(usize, Row)> {
    let cl3 = df.column("column_3")?;
    let arg_min = cl3.arg_min().unwrap();
    let _arg_min_value = cl3.get(arg_min)?;
    let min_row = df.get_row(arg_min)?;
    Ok((arg_min, min_row))
}

fn write_result(results: &[Vec<String>]) -> Result<()> {
    let mut writer = csv::Writer::from_writer(io::stdout());
    for row_values in results {
        writer.write_record(row_values)?;
    }
    Ok(())
}

fn process_csv<P>(path: P) -> Result<Vec<String>>
where
    P: AsRef<Path>,
{
    let df = load_polars_from_path(path.as_ref())
        .context(format!("error loading csv file: {:?}", path.as_ref()))?;

    let (row_index, row) = find_minimum_value_in_third_column(&df)?;

    let row_values = row.0.iter().map(|x| x.to_string()).collect::<Vec<String>>();

    let row_values = row_values.join(" ");

    let file_name = path
        .as_ref()
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_string();

    let result = vec![file_name, (row_index + 1).to_string(), row_values];
    Ok(result)
}

fn read_jld<P>(jld_path: P) -> Result<()> {
    use Color::*;
    let file = File::open(jld_path.as_ref())?; // open for reading
    let ds = file.dataset("dir/pixels")?; // open the dataset
    Ok(())
}

fn worker<P>(folder: P, max_files: Option<usize>) -> Result<()>
where
    P: AsRef<Path>,
{
    info!("collect jld paths from folder: {:?}", folder.as_ref());
    let mut jld_paths = find_jld_paths(folder)?;
    info!("found {} jld files", jld_paths.len());

    if let Some(max_files) = max_files {
        info!("truncate csv files to: {} files", max_files);
        jld_paths.truncate(max_files);
    }

    let result = read_jld(jld_paths.iter().next().unwrap()).unwrap();

    // let results = jld_paths
    //     .iter()
    //     .filter_map(|path| match process_csv(path) {
    //         Ok(result) => Some(result),
    //         Err(e) => {
    //             log::error!("error: {:?}", e);
    //             None
    //         }
    //     })
    //     .collect::<Vec<Vec<String>>>();

    // info!("found {} results", results.len());

    // write_result(&results)?;
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
    worker(cli.folder, cli.max_files)?;

    let elapsed = start.elapsed();
    log::info!("elapsed time: {:.2?}", elapsed);
    Ok(())
}
