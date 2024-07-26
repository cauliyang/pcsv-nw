use ahash::HashMap;
use ahash::HashMapExt;
use anyhow::Result;
use clap::Parser;
use csv::Reader;
use rayon::prelude::*;
use std::io;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(version, about = "Summary CSV", long_about = None)]
struct Cli {
    /// path to the csvs
    #[arg(long = "csv", value_name = "csvs", action = clap::ArgAction::Append)]
    csvs: Vec<PathBuf>,

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

fn test() {
    let mut rdr = csv::Reader::from_reader(io::stdin());
    for result in rdr.records() {
        let record = result.expect("a CSV record");
        println!("{:?}", record);
    }
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

    test();

    let elapsed = start.elapsed();
    log::info!("elapsed time: {:.2?}", elapsed);
    Ok(())
}
