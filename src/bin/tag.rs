#![feature(async_fn_in_trait)]

use std::{
    cell::RefCell,
    mem::{transmute, MaybeUninit},
    rc::Rc,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use anyhow::Context;
use bench_async_ucx::Config;
use metrics::atomics::AtomicU64;
use tracing::info;
extern crate mpi;

#[derive(Clone)]
struct Bench {
    io_count: Arc<AtomicU64>,
    tag: Arc<AtomicU64>,
}

impl bench_async_ucx::Bench for Bench {
    async fn client(&self, ep: async_ucx::ucp::Endpoint) -> anyhow::Result<()> {
        let mut tag_buf = [MaybeUninit::uninit(); 8];
        let worker = ep.worker();
        worker.tag_recv(100, &mut tag_buf).await?;
        let tag: u64 = unsafe { transmute(tag_buf) };
        let msg = "Hello World".as_bytes().to_vec();
        let mut buf = [MaybeUninit::uninit(); 256];
        for _ in 0..50_000_000 {
            ep.tag_send(tag, &msg).await?;
            worker.tag_recv(tag, &mut buf).await?;
        }
        info!("client_exit_successfully");
        Ok(())
    }
    async fn server(&self, ep: async_ucx::ucp::Endpoint) -> anyhow::Result<()> {
        let tag = self.tag.fetch_add(1, Ordering::SeqCst);
        ep.tag_send(100, tag.to_ne_bytes().as_ref()).await?;
        let mut buf = [MaybeUninit::uninit(); 256];
        let worker = ep.worker();
        let io_count = self.io_count.clone();
        let local_count = Rc::new(RefCell::new(0));
        let local_count_ref = local_count.clone();
        tokio::task::spawn_local(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let counted: u64 = *local_count_ref.borrow();
                *local_count_ref.borrow_mut() = 0;
                io_count.fetch_add(counted, Ordering::SeqCst);
            }
        });
        for _ in 0..1_00_000 {
            worker.tag_recv(tag, &mut buf).await?;
            ep.tag_send(tag, unsafe { transmute(&buf[..]) }).await?;
            *local_count.borrow_mut() += 1;
        }
        info!("server_exit_successfully");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server_thread_count = std::env::var("SERVER_THREAD_COUNT")
        .with_context(|| "SERVER_THREAD_COUNT")?
        .parse()?;
    let client_thread_count = std::env::var("CLIENT_THREAD_COUNT")
        .with_context(|| "CLIENT_THREAD_COUNT")?
        .parse()?;
    let client_task_count = std::env::var("CLIENT_TASK_COUNT")
        .with_context(|| "CLIENT_TASK_COUNT")?
        .parse()?;
    let io_count = Arc::new(AtomicU64::new(0));
    let tag = Arc::new(AtomicU64::new(1000));
    let io_count_ = io_count.clone();
    info!(
        server_thread_count,
        client_thread_count, client_task_count, "init"
    );
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let iops = io_count_.swap(0, Ordering::SeqCst);
            if iops > 0 {
                info!(iops, "IOPS");
            }
        }
    });
    tokio::spawn(async move {
        // deadline 10m
        tokio::time::sleep(Duration::from_secs(60 * 5)).await;
        std::process::exit(1);
    });
    bench_async_ucx::bench(
        Bench { io_count, tag },
        Config {
            server_thread_count,
            client_thread_count,
            client_task_count,
        },
    )
    .await?;
    Ok(())
}
