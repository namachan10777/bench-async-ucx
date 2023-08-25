extern crate mpi;

use std::any;
use std::net::Ipv4Addr;

use anyhow::Context;
use mpi::request::WaitGuard;
use mpi::traits::*;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();
    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let rank = world.rank();

    info!(size=size, rank=rank, "mpi_init");

    if rank == 0 {
        let child = world.process_at_rank(1);
        let ip = bench_async_ucx::get_ip_candidates()?.pop().with_context(|| "ip not found")?;
        child.send_with_tag(ip.octets().as_ref(), 99);
        info!(ip=ip.to_string(), "send_ip");
    }
    else {
        let mut buf = [0; 4];
        let root = world.process_at_rank(0);
        let status = root.receive_into_with_tag(&mut buf, 99);
        let ip = Ipv4Addr::from(buf);
        info!(ip=ip.to_string(), source_rank=status.source_rank(), tag=status.tag(), "get_ip");
    }
    Ok(())
}
