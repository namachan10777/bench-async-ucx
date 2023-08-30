#![feature(async_fn_in_trait)]

use std::{sync::{Arc, atomic::Ordering}, mem::{MaybeUninit, transmute}, time::Duration, rc::Rc, cell::RefCell};

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
        for _ in 0..10_000_000 {
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
            tokio::time::sleep(Duration::from_millis(100)).await;
            let counted = *local_count_ref.borrow();
            *local_count_ref.borrow_mut() = 0;
            io_count.fetch_add(counted, Ordering::SeqCst);
        });
        for _ in 0..10_000_000 {
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
    let io_count = Arc::new(AtomicU64::new(0));
    let tag = Arc::new(AtomicU64::new(1000));
    let io_count_ = io_count.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let iops = io_count_.swap(0, Ordering::SeqCst);
            if iops > 0 {
                info!(iops, "IOPS");
            }
        }
    });
    bench_async_ucx::bench(Bench {
        io_count,
        tag
    }, Config {
        server_thread_count: 1,
        client_thread_count: 1,
        client_task_count: 32,
    }).await?;
    Ok(())
}