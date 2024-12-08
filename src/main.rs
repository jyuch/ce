mod convert;

use crate::convert::convert;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Cli {
    #[clap(subcommand)]
    action: MainAction,
}

impl Cli {
    fn handle(self) -> anyhow::Result<()> {
        match self.action {
            MainAction::Convert {
                input,
                output,
                sort,
            } => convert(input, output, sort),
        }
    }
}

#[derive(Subcommand, Debug)]
enum MainAction {
    /// Convert file format.
    #[clap(name = "conv")]
    Convert {
        /// Input.
        #[clap(short, long)]
        input: PathBuf,
        /// Output.
        #[clap(short, long)]
        output: PathBuf,
        /// Sort.
        #[clap(short, long, default_value_t = false)]
        sort: bool,
    },
}

fn main() -> anyhow::Result<()> {
    let opt = Cli::parse();
    opt.handle()?;
    Ok(())
}
