use std::{
    mem::{transmute, MaybeUninit},
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Context as _};
use async_ucx::ucp::{ConnectionRequest, Context, Endpoint, MemoryHandle, RKey};
use clap::Parser;
use tokio::{
    sync::mpsc::{channel, Sender},
    task::LocalSet,
};
use tracing::{debug, error, info};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};
use mpi::traits::*;

#[derive(Parser)]
struct Opts {
    thread: usize,
    task_per_thread: usize,
    buffer_size: usize,
}

struct WorkerThread {
    queue: Sender<ConnectionRequest>,
}

impl WorkerThread {
    fn new<T: 'static, F: Send + Sync + 'static + for<'a> Fn(&'a Endpoint) -> T>(
        ucx_ctx: Arc<Context>,
        make_ctx: F,
        handler: fn(T, Endpoint) -> anyhow::Result<()>,
    ) -> Self {
        let (tx, mut rx) = channel(1024);
        std::thread::spawn({
            move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let local = LocalSet::new();
                let worker = ucx_ctx.create_worker()?;
                local.spawn_local(worker.clone().polling());
                local.block_on(&rt, async move {
                    while let Some(conn_req) = rx.recv().await {
                        let ep = worker.accept(conn_req).await?;
                        ep.print_to_stderr();
                        let ctx = make_ctx(&ep);
                        handler(ctx, ep)?;
                    }
                    Ok::<_, anyhow::Error>(())
                })?;
                Ok::<_, anyhow::Error>(())
            }
        });
        info!("worker_thread_created");
        Self { queue: tx }
    }

    async fn accept(&self, conn_req: ConnectionRequest) -> anyhow::Result<()> {
        self.queue.send(conn_req).await.unwrap();
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    info!("logger_initialized");
    let opts = Opts::parse();
    let local = tokio::task::LocalSet::new();

    let universe = mpi::initialize().unwrap();
    let world = universe.world();
    let size = world.size();
    let rank = world.rank();

    info!(size = size, rank = rank, "mpi_init");

    if rank == 0 {
        let child = world.process_at_rank(1);
        let ip = bench_async_ucx::get_ip_candidates()?
            .pop()
            .with_context(|| "ip not found")?;
        child.send_with_tag(ip.octets().as_ref(), 99);
        info!(ip = ip.to_string(), "send_ip");
        local
            .run_until(
                server(
                    SocketAddr::V4(SocketAddrV4::new(ip, 10032)),
                    opts.buffer_size,
                    opts.thread,
                ),
            )
            .await
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
        local.run_until(
            client(
                SocketAddr::V4(SocketAddrV4::new(ip, 10032)),
                opts.buffer_size,
                opts.thread,
                opts.task_per_thread,
            ),
        ).await
    }
}

async fn client(
    addr: SocketAddr,
    buffer_size: usize,
    threads: usize,
    task_per_thread: usize,
) -> anyhow::Result<()> {
    let ctx = Context::new()?;
    ctx.print_to_stderr();
    info!("context_created");
    for _ in 0..threads {
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let local = tokio::task::LocalSet::new();
            let worker = ctx.create_worker()?;
            info!("worker_created");
            local.spawn_local(worker.clone().polling());
            local.block_on(&rt, async move {
                for _ in 0..task_per_thread {
                    let worker = worker.clone();
                    let ctx = ctx.clone();
                    tokio::task::spawn_local(async move {
                        let ep = worker.connect_socket(addr).await?;
                        debug!(addr = addr.to_string(), "server_connected");
                        loop {
                            client_task(ctx.clone(), &ep, buffer_size).await?;
                        }
                        // write client code
                        #[allow(unreachable_code)]
                        Ok::<_, anyhow::Error>(())
                    });
                }
                loop {
                    // infinite loop
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            });
            bail!("client thread exited");
            #[allow(unreachable_code)]
            Ok(())
        });
    }
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn client_task(ctx: Arc<Context>, ep: &Endpoint, buffer_size: usize) -> anyhow::Result<()> {
    debug!("start_client_task");
    // send code
    let send_buffer_size_buf = buffer_size.to_ne_bytes().to_vec();

    let mut send_buffer = vec![0xabu8; buffer_size];
    let vaddr = send_buffer.as_ptr() as u64;
    let memory = MemoryHandle::register(&ctx, &mut send_buffer);

    ep.stream_send(&send_buffer_size_buf).await?;
    debug!(buffer_size, "sent_buffer_size");
    ep.stream_send(&(vaddr.to_ne_bytes())).await?;
    debug!(vaddr = vaddr, "sent_vaddr");
    ep.stream_send(memory.pack().as_ref()).await?;
    debug!("sent_rkey");

    let mut recv_buffer_size_buf = [MaybeUninit::uninit(); 8];
    ep.stream_recv(&mut recv_buffer_size_buf).await?;
    let recv_buffer_size_ptr =
        unsafe { transmute::<_, *const usize>(recv_buffer_size_buf.as_ptr()) };
    let recv_buffer_size = unsafe { *recv_buffer_size_ptr };
    debug!(buffer_size = recv_buffer_size, "got_server_buffer_size");

    let mut recv_buffer = vec![0u8; recv_buffer_size];

    let mut vaddr_buffer = [MaybeUninit::uninit(); 8];
    ep.stream_recv(&mut vaddr_buffer).await?;
    let vaddr_ptr = unsafe { transmute::<_, *const u64>(vaddr_buffer.as_ptr()) };
    let vaddr = unsafe { *vaddr_ptr };
    debug!(vaddr = vaddr, "got_vaddr");

    let mut rkey_buffer = [MaybeUninit::uninit(); 256];
    let rkey_len = ep.stream_recv(&mut rkey_buffer).await?;
    let rkey = RKey::unpack(&ep, unsafe {
        transmute(AsRef::<[MaybeUninit<u8>]>::as_ref(&rkey_buffer[..rkey_len]))
    });
    debug!("got_rkey");

    ep.get(&mut recv_buffer, vaddr, &rkey).await?;
    debug!("got_remote_memory");
    let message = "Success!".as_bytes();
    ep.stream_send(&message).await?;
    debug!("sent_msg");
    Ok(())
}

async fn server_task(ctx: Arc<Context>, ep: &Endpoint, buffer_size: usize) -> anyhow::Result<()> {
    let mut buffer_size_buf = [MaybeUninit::uninit(); 8];
    let mut vaddr_buf = [MaybeUninit::uninit(); 8];
    let mut rkey_buf = [MaybeUninit::uninit(); 256];

    ep.stream_recv(&mut buffer_size_buf).await?;
    let client_buffer_size = unsafe { *transmute::<_, *const u64>(buffer_size_buf.as_ptr()) };
    debug!(buffer_size = client_buffer_size, "client_buffer_size_recv");

    ep.stream_recv(&mut vaddr_buf).await?;
    let vaddr = unsafe { *transmute::<_, *const u64>(vaddr_buf.as_ptr()) };
    debug!(vaddr = vaddr, "client_vaddr_recv");

    let rkey_len = ep.stream_recv(&mut rkey_buf).await?;
    let rkey = RKey::unpack(&ep, unsafe {
        transmute::<_, &[u8]>(AsRef::<[MaybeUninit<u8>]>::as_ref(&rkey_buf[..rkey_len]))
    });
    debug!("rkey_recv");

    let mut recv_buffer = vec![0u8; client_buffer_size as usize];
    ep.get(&mut recv_buffer, vaddr, &rkey).await?;
    debug!("got_remote_memory");

    let mut send_buffer = vec![0u8; buffer_size];
    let memory = MemoryHandle::register(&ctx, &mut send_buffer);

    ep.stream_send(&buffer_size.to_ne_bytes()).await?;
    debug!(buffer_size = buffer_size, "sent_buffer_size");
    let vaddr = send_buffer.as_ptr() as u64;
    ep.stream_send(&vaddr.to_ne_bytes()).await?;
    debug!(vaddr = vaddr, "sent_vaddr");
    ep.stream_send(memory.pack().as_ref()).await?;
    debug!("sent_rkey");

    let mut msg = [MaybeUninit::uninit(); 256];
    let len = ep.stream_recv(&mut msg).await?;
    let msg = std::str::from_utf8(unsafe { transmute::<&[MaybeUninit<u8>], &[u8]>(&msg[..len]) })?;
    debug!(msg = msg, "got_msg");
    Ok(())
}

#[derive(Clone)]
struct ServerContext {
    ctx: Arc<Context>,
    iop_counter: Arc<AtomicU64>,
    buffer_size: usize,
}

async fn server(addr: SocketAddr, buffer_size: usize, threads: usize) -> anyhow::Result<()> {
    let ctx = Context::new()?;
    ctx.print_to_stderr();
    info!("context_created");
    let worker = ctx.create_worker()?;

    let user_ctx = ServerContext {
        ctx: ctx.clone(),
        iop_counter: Arc::new(AtomicU64::new(0)),
        buffer_size,
    };

    let workers = (0..threads)
        .map(|_| {
            let user_ctx = user_ctx.clone();
            WorkerThread::new(
                ctx.clone(),
                move |_| user_ctx.clone(),
                |ctx: ServerContext, ep| {
                    tokio::task::spawn_local(async move {
                        loop {
                            // write real communication code
                            if let Err(e) = server_task(ctx.ctx.clone(), &ep, ctx.buffer_size).await
                            {
                                error!(e = e.to_string(), "msg");
                            }
                            ctx.iop_counter.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                    Ok(())
                },
            )
        })
        .collect::<Vec<_>>();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            info!(iops = user_ctx.iop_counter.load(Ordering::Relaxed), "iops");
            user_ctx.iop_counter.store(0, Ordering::SeqCst);
        }
    });

    let mut listener = worker.create_listener(addr)?;
    debug!(addr = addr.to_string(), "listener_created");
    tokio::task::spawn_local(worker.clone().polling());
    for idx in 0.. {
        debug!(id = idx, "waiting_request");
        let conn_req = listener.next().await;
        debug!(
            from = conn_req.remote_addr()?.to_string(),
            "incoming_request"
        );
        workers[idx % workers.len()].accept(conn_req).await?;
    }
    info!(progress = worker.progress(), "finish_server");
    Ok::<_, anyhow::Error>(())
}
