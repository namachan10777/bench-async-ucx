use std::{
    mem::{transmute, MaybeUninit},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use anyhow::Context as _;
use bench_sync_ucx::*;
use mpi::traits::{Communicator, Destination, Source};
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use regex::Regex;
use tracing::{info, warn};

pub fn get_ip_candidates() -> anyhow::Result<Vec<Ipv4Addr>> {
    let regex = Regex::new(r#"^ib.+$|^enp.+$"#)?;
    let ifaces = NetworkInterface::show()?;
    let ips = ifaces
        .into_iter()
        .filter(|iface| regex.is_match(&iface.name))
        .flat_map(|iface| iface.addr)
        .map(|addr| addr.ip())
        .filter_map(|addr| {
            if let IpAddr::V4(addr) = addr {
                Some(addr)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    Ok(ips)
}

const MESSAGE: &str = "Hello World!\0";

unsafe fn client_server_do_work(ep: Endpoint, is_server: bool) -> Result<(), Error> {
    if is_server {
        let mut buffer = [MaybeUninit::uninit(); 256];
        let rx_cb = Rc::new(|_| {});
        let tx_cb = Rc::new(|_| {});

        for _ in 0..50 {
            for _ in 0..100_000 {
                let status = ep.tag_recv(&mut buffer, 99, 0, Rc::downgrade(&rx_cb));
                status.wait(&ep.worker)?;
                let buffer: [u8; 256] = transmute(buffer);
                let status = ep.tag_send(101, buffer, Rc::downgrade(&tx_cb));
                status.wait(&ep.worker)?;
            }
        }

        Ok(())
    } else {
        let rx_cb = Rc::new(|_| {});
        let tx_cb = Rc::new(|_| {});

        for _ in 0..50 {
            let now = Instant::now();
            for _ in 0..100_000 {
                let status = ep.tag_send(99, MESSAGE.as_bytes(), Rc::downgrade(&tx_cb));
                status.wait(&ep.worker)?;

                let mut buffer = [MaybeUninit::uninit(); 256];
                let status = ep.tag_recv(&mut buffer, 101, 0, Rc::downgrade(&rx_cb));
                status.wait(&ep.worker)?;
            }
            let elapsed = now.elapsed().as_micros();
            info!(iops = (100000.0 / elapsed as f64 * 1000.0 * 1000.0))
        }

        Ok(())
    }
}

unsafe fn conn_handler(conn_req: ConnectionRequest, worker: Rc<Worker>, state: Rc<AtomicBool>) {
    let ep = match Endpoint::from_conn_req(worker, conn_req) {
        Ok(ep) => ep,
        Err(e) => {
            warn!("{e}");
            return;
        }
    };
    if let Err(e) = client_server_do_work(ep, true) {
        warn!("{e}");
    }
    state.store(true, Ordering::SeqCst);
}

fn main() -> anyhow::Result<()> {
    use tracing_subscriber::prelude::*;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "Info".into()),
        )
        .with(tracing_subscriber::fmt::Layer::default().with_ansi(true))
        .init();

    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let rank = world.rank();
    info!(size = size, rank = rank, "mpi_init");
    let port = 10301;

    if rank == 0 {
        let child = world.process_at_rank(1);
        let ip = get_ip_candidates()?.pop().with_context(|| "ip not found")?;
        child.send_with_tag(ip.octets().as_ref(), 99);
        info!(ip = ip.to_string(), "send_ip");
        unsafe {
            let ctx = Context::init()?;
            let worker = Worker::create(&ctx)?;
            let end_flag = Rc::new(AtomicBool::new(false));
            let _ = Listener::create(
                &worker,
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port)),
                conn_handler,
                end_flag.clone(),
            )?;
            while !end_flag.load(Ordering::Relaxed) {
                worker.progress();
            }
        };
        info!("server_exited");
    } else {
        let mut buf = [0; 4];
        let root = world.process_at_rank(0);
        let status = root.receive_into_with_tag(&mut buf, 99);
        let ip = Ipv4Addr::from(buf);
        info!(
            ip = ip.to_string(),
            source_rank = status.source_rank(),
            tag = status.tag(),
            "get_ip"
        );
        std::thread::sleep(Duration::from_secs(1));
        unsafe {
            let ctx = Context::init()?;
            let worker = Worker::create(&ctx)?;
            let ep = match Endpoint::from_sockaddr(
                worker,
                SocketAddr::V4(SocketAddrV4::new(ip, port)),
            ) {
                Ok(ep) => ep,
                Err(e) => {
                    warn!("{e}");
                    return Err(e.into());
                }
            };
            if let Err(e) = client_server_do_work(ep, false) {
                warn!("{e}");
            }
        }
        info!("client_exited");
    }
    Ok(())
}
