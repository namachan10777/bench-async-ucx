#![feature(async_fn_in_trait)]
use anyhow::Context as _;
use async_ucx::ucp::{ConnectionRequest, Context, Endpoint};
use metrics::atomics::AtomicU64;
use mpi::traits::*;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};
use tracing_subscriber::prelude::*;

use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use regex::Regex;

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

pub trait Bench {
    async fn server(&self, ep: Endpoint) -> anyhow::Result<()>;
    async fn client(&self, ep: Endpoint) -> anyhow::Result<()>;
}

struct WorkerThread {
    sender: mpsc::UnboundedSender<ConnectionRequest>,
}

impl WorkerThread {
    fn new<B: Bench + Clone + Send + Sync + 'static>(
        ctx: &Arc<Context>,
        bench: B,
        exit: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        let context = ctx.clone();
        let (sender, mut recver) = mpsc::unbounded_channel::<ConnectionRequest>();
        std::thread::spawn(move || {
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel(1024);
            let worker = context.create_worker().unwrap();
            let rt: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            let (shutdown_runtime_tx, mut shutdown_runtime_rx) = tokio::sync::oneshot::channel();

            let local = tokio::task::LocalSet::new();
            #[cfg(not(event))]
            local.spawn_local(worker.clone().polling());
            #[cfg(feature = "event")]
            local.spawn_local(worker.clone().event_poll());

            let task_count = Arc::new(AtomicU64::new(0));

            let (accept_at_least_one_tx, accept_at_least_one_rx) = oneshot::channel();

            local.spawn_local({
                let task_count = task_count.clone();
                async move {
                    accept_at_least_one_rx.await.unwrap();
                    let mut shutdown_count = 0;
                    while let Ok(_) = shutdown_rx.recv().await {
                        shutdown_count += 1;
                        if shutdown_count >= task_count.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                    shutdown_runtime_tx.send(()).unwrap();
                }
            });

            local.block_on(&rt, async move {
                let mut accept = Some(accept_at_least_one_tx);
                loop {
                    tokio::select! {
                        Some(conn) = recver.recv() => {
                            task_count.fetch_add(1, Ordering::SeqCst);
                            accept.take().map(|tx| tx.send(()));
                            let ep = worker.accept(conn).await.unwrap();
                            let bench = bench.clone();
                            let shutdown_tx = shutdown_tx.clone();
                            tokio::task::spawn_local(async move {
                                if let Err(e) = bench.server(ep).await {
                                    warn!(e=e.to_string(), "server_error");
                                }
                                shutdown_tx.send(()).unwrap();
                            });
                        }
                        _ = &mut shutdown_runtime_rx => {
                            break;
                        }
                    }
                }
            });
            exit.send(()).unwrap();
        });
        WorkerThread { sender }
    }
}

pub struct Config {
    pub server_thread_count: usize,
    pub client_thread_count: usize,
    pub client_task_count: usize,
}

pub async fn bench<B: Bench + Clone + Send + Sync + 'static>(
    bench: B,
    config: Config,
) -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "Info".into()),
        )
        .with(tracing_subscriber::fmt::Layer::default().with_ansi(false))
        .init();

    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let rank = world.rank();
    info!(size = size, rank = rank, "mpi_init");
    let local = tokio::task::LocalSet::new();
    let port = 10032;

    if rank == 0 {
        let child = world.process_at_rank(1);
        let ip = get_ip_candidates()?.pop().with_context(|| "ip not found")?;
        child.send_with_tag(ip.octets().as_ref(), 99);
        info!(ip = ip.to_string(), "send_ip");
        let mut shutdowns = Vec::new();
        let mut workers = Vec::new();
        let ctx = Context::new()?;
        let worker = ctx.create_worker()?;
        let mut listener = worker.create_listener(SocketAddr::V4(SocketAddrV4::new(ip, port)))?;
        trace!(on = ip.to_string(), "listener_created");
        local.spawn_local(worker.clone().polling());
        for _ in 0..config.server_thread_count {
            let (exit, exit_wait) = oneshot::channel();
            let worker = WorkerThread::new(&ctx, bench.clone(), exit);
            shutdowns.push(exit_wait);
            workers.push(worker);
        }
        let shutdown_receiver = futures::future::join_all(shutdowns);
        trace!("spawn_server_watcher");
        local.spawn_local(async move {
            for idx in 0.. {
                let conn_req: ConnectionRequest = listener.next().await;
                debug!("got_conn_req");
                workers[idx % workers.len()].sender.send(conn_req).unwrap();
            }
        });
        trace!("spawn_connection_handler");
        local.run_until(shutdown_receiver).await;
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
        let mut thread_handlers = Vec::new();
        let ctx = Context::new()?;
        for _ in 0..config.client_thread_count {
            let task_count = config.client_task_count;
            let ctx = ctx.clone();
            let bench = bench.clone();
            let handler = std::thread::spawn(move || {
                let worker = ctx.create_worker().unwrap();
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                let local = tokio::task::LocalSet::new();
                #[cfg(not(event))]
                local.spawn_local(worker.clone().polling());
                #[cfg(feature = "event")]
                local.spawn_local(worker.clone().event_poll());
                let mut task_handlers = Vec::new();
                for _ in 0..task_count {
                    let worker = worker.clone();
                    let bench = bench.clone();
                    let handler = local.spawn_local(async move {
                        let ep = worker
                            .connect_socket(SocketAddr::V4(SocketAddrV4::new(ip, port)))
                            .await
                            .unwrap();
                        debug!("spawn_client");
                        bench.client(ep).await
                    });
                    task_handlers.push(handler);
                }
                trace!(to = ip.to_string(), "client_created");
                local.block_on(&rt, futures::future::join_all(task_handlers));
            });
            thread_handlers.push(handler);
        }
    }
    Ok(())
}
