use anyhow::{Context, Result};
use clap::Parser;
use raptorq_datagram_fec::{UdpFecSender, DEFAULT_SOURCE_SYMBOLS, DEFAULT_SYMBOL_SIZE};
use std::net::SocketAddr;
use tokio::io::{self, AsyncReadExt};

#[derive(Debug, Parser)]
#[command(
    name = "udp-fec-send",
    about = "Send stdin to an av-mesh UDP-FEC ingest socket"
)]
struct Args {
    target: SocketAddr,

    #[arg(long, default_value_t = 1)]
    repair_symbols: u32,

    #[arg(long, default_value_t = DEFAULT_SYMBOL_SIZE)]
    symbol_size: u16,

    #[arg(long)]
    chunk_bytes: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut input = Vec::new();
    io::stdin()
        .read_to_end(&mut input)
        .await
        .context("failed to read stdin")?;

    let chunk_bytes = args
        .chunk_bytes
        .unwrap_or(DEFAULT_SOURCE_SYMBOLS as usize * args.symbol_size as usize)
        .max(1);
    let mut sender = UdpFecSender::new(args.target)
        .await
        .with_context(|| format!("failed to create UDP-FEC sender for {}", args.target))?
        .with_repair_symbols(args.repair_symbols)
        .with_symbol_size(args.symbol_size);

    for chunk in input.chunks(chunk_bytes) {
        sender
            .send(chunk)
            .await
            .with_context(|| format!("failed to send UDP-FEC chunk to {}", args.target))?;
    }

    println!(
        "sent {} bytes to {} using UDP-FEC chunks of {} bytes",
        input.len(),
        args.target,
        chunk_bytes
    );
    Ok(())
}
