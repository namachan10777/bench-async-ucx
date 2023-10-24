use std::{
    cell::RefCell,
    ffi::c_void,
    mem::{transmute, MaybeUninit},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    rc::{Rc, Weak},
    sync::atomic::{AtomicBool, Ordering},
};

use anyhow::Context as _;
use mpi::traits::{Communicator, Destination, Source};
use network_interface::{NetworkInterface, NetworkInterfaceConfig};
use regex::Regex;
use tracing::{error, info, trace, warn};
use ucx1_sys::{
    ucp_cleanup, ucp_config_read, ucp_conn_request, ucp_context_h, ucp_ep_close_flags_t,
    ucp_ep_close_nbx, ucp_ep_create, ucp_ep_destroy, ucp_ep_h, ucp_ep_params, ucp_ep_params_field,
    ucp_err_handler, ucp_feature, ucp_init_version, ucp_listener_create, ucp_listener_destroy,
    ucp_listener_h, ucp_listener_params_field, ucp_listener_params_t, ucp_op_attr_t,
    ucp_request_check_status, ucp_request_free, ucp_request_param_t,
    ucp_request_param_t__bindgen_ty_1, ucp_tag_recv_info, ucp_tag_send_nbx, ucp_worker_create,
    ucp_worker_destroy, ucp_worker_h, ucp_worker_progress, ucs_sock_addr, ucs_status_ptr_t,
    ucs_status_t, UCS_PTR_IS_ERR, UCS_PTR_RAW_STATUS, UCS_PTR_STATUS,
};

/// UCX error code.
#[allow(missing_docs)]
#[repr(i8)]
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Operation in progress")]
    Inprogress,
    #[error("No pending message")]
    NoMessage,
    #[error("No resources are available to initiate the operation")]
    NoReource,
    #[error("Input/output error")]
    IoError,
    #[error("Out of memory")]
    NoMemory,
    #[error("Invalid parameter")]
    InvalidParam,
    #[error("Destination is unreachable")]
    Unreachable,
    #[error("Address not valid")]
    InvalidAddr,
    #[error("Function not implemented")]
    NotImplemented,
    #[error("Message truncated")]
    MessageTruncated,
    #[error("No progress")]
    NoProgress,
    #[error("Provided buffer is too small")]
    BufferTooSmall,
    #[error("No such element")]
    NoElem,
    #[error("Failed to connect some of the requested endpoints")]
    SomeConnectsFailed,
    #[error("No such device")]
    NoDevice,
    #[error("Device is busy")]
    Busy,
    #[error("Request canceled")]
    Canceled,
    #[error("Shared memory error")]
    ShmemSegment,
    #[error("Element already exists")]
    AlreadyExists,
    #[error("Index out of range")]
    OutOfRange,
    #[error("Operation timed out")]
    Timeout,
    #[error("User-defined limit was reached")]
    ExceedsLimit,
    #[error("Unsupported operation")]
    Unsupported,
    #[error("Operation rejected by remote peer")]
    Rejected,
    #[error("Endpoint is not connected")]
    NotConnected,
    #[error("Connection reset by remote peer")]
    ConnectionReset,

    #[error("First link failure")]
    FirstLinkFailure,
    #[error("Last link failure")]
    LastLinkFailure,
    #[error("First endpoint failure")]
    FirstEndpointFailure,
    #[error("Last endpoint failure")]
    LastEndpointFailure,
    #[error("Endpoint timeout")]
    EndpointTimeout,

    #[error("Unknown error")]
    Unknown,
}

impl Error {
    // status != UCS_OK
    fn from_error(status: ucs_status_t) -> Self {
        debug_assert_ne!(status, ucs_status_t::UCS_OK);

        match status {
            ucs_status_t::UCS_INPROGRESS => Self::Inprogress,
            ucs_status_t::UCS_ERR_NO_MESSAGE => Self::NoMessage,
            ucs_status_t::UCS_ERR_NO_RESOURCE => Self::NoReource,
            ucs_status_t::UCS_ERR_IO_ERROR => Self::IoError,
            ucs_status_t::UCS_ERR_NO_MEMORY => Self::NoMemory,
            ucs_status_t::UCS_ERR_INVALID_PARAM => Self::InvalidParam,
            ucs_status_t::UCS_ERR_UNREACHABLE => Self::Unreachable,
            ucs_status_t::UCS_ERR_INVALID_ADDR => Self::InvalidAddr,
            ucs_status_t::UCS_ERR_NOT_IMPLEMENTED => Self::NotImplemented,
            ucs_status_t::UCS_ERR_MESSAGE_TRUNCATED => Self::MessageTruncated,
            ucs_status_t::UCS_ERR_NO_PROGRESS => Self::NoProgress,
            ucs_status_t::UCS_ERR_BUFFER_TOO_SMALL => Self::BufferTooSmall,
            ucs_status_t::UCS_ERR_NO_ELEM => Self::NoElem,
            ucs_status_t::UCS_ERR_SOME_CONNECTS_FAILED => Self::SomeConnectsFailed,
            ucs_status_t::UCS_ERR_NO_DEVICE => Self::NoDevice,
            ucs_status_t::UCS_ERR_BUSY => Self::Busy,
            ucs_status_t::UCS_ERR_CANCELED => Self::Canceled,
            ucs_status_t::UCS_ERR_SHMEM_SEGMENT => Self::ShmemSegment,
            ucs_status_t::UCS_ERR_ALREADY_EXISTS => Self::AlreadyExists,
            ucs_status_t::UCS_ERR_OUT_OF_RANGE => Self::OutOfRange,
            ucs_status_t::UCS_ERR_TIMED_OUT => Self::Timeout,
            ucs_status_t::UCS_ERR_EXCEEDS_LIMIT => Self::ExceedsLimit,
            ucs_status_t::UCS_ERR_UNSUPPORTED => Self::Unsupported,
            ucs_status_t::UCS_ERR_REJECTED => Self::Rejected,
            ucs_status_t::UCS_ERR_NOT_CONNECTED => Self::NotConnected,
            ucs_status_t::UCS_ERR_CONNECTION_RESET => Self::ConnectionReset,

            ucs_status_t::UCS_ERR_FIRST_LINK_FAILURE => Self::FirstLinkFailure,
            ucs_status_t::UCS_ERR_LAST_LINK_FAILURE => Self::LastLinkFailure,
            ucs_status_t::UCS_ERR_FIRST_ENDPOINT_FAILURE => Self::FirstEndpointFailure,
            ucs_status_t::UCS_ERR_ENDPOINT_TIMEOUT => Self::EndpointTimeout,
            ucs_status_t::UCS_ERR_LAST_ENDPOINT_FAILURE => Self::LastEndpointFailure,

            _ => Self::Unknown,
        }
    }

    #[inline]
    fn from_status(status: ucs_status_t) -> Result<(), Self> {
        if status == ucs_status_t::UCS_OK {
            Ok(())
        } else {
            Err(Self::from_error(status))
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn from_ptr(ptr: ucs_status_ptr_t) -> Result<(), Self> {
        if UCS_PTR_IS_ERR(ptr) {
            Err(Self::from_error(UCS_PTR_RAW_STATUS(ptr)))
        } else {
            Ok(())
        }
    }
}

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

struct Worker {
    h: ucp_worker_h,
    _ctx: Rc<Context>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        unsafe {
            trace!("drop worker");
            ucp_worker_destroy(self.h);
        }
    }
}

struct ConnHandlerData<S> {
    cb: unsafe fn(ConnectionRequest, Rc<Worker>, S),
    state: S,
    worker: Rc<Worker>,
}

impl Worker {
    unsafe fn create(ctx: &Rc<Context>) -> Result<Rc<Worker>, Error> {
        let worker_params_default = MaybeUninit::uninit();
        let worker_params = ucx1_sys::ucp_worker_params_t {
            field_mask: ucx1_sys::ucp_worker_params_field::UCP_WORKER_PARAM_FIELD_THREAD_MODE.0
                as u64,
            thread_mode: ucx1_sys::ucs_thread_mode_t::UCS_THREAD_MODE_SINGLE,
            ..unsafe { worker_params_default.assume_init() }
        };
        let mut worker = MaybeUninit::uninit();
        let status = ucp_worker_create(ctx.h, &worker_params, worker.as_mut_ptr());
        Error::from_status(status)?;
        let worker = worker.assume_init();
        trace!("worker_created");
        Ok(Rc::new(Worker {
            h: worker,
            _ctx: ctx.clone(),
        }))
    }

    unsafe fn progress(&self) -> u32 {
        ucp_worker_progress(self.h) as _
    }
}

struct Context {
    h: ucp_context_h,
}
impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            trace!("drop context");
            ucp_cleanup(self.h);
        }
    }
}

impl Context {
    unsafe fn init() -> Result<Rc<Self>, Error> {
        let params_default = MaybeUninit::uninit();
        let params = ucx1_sys::ucp_params_t {
            field_mask: ucx1_sys::ucp_params_field::UCP_PARAM_FIELD_FEATURES.0 as u64,
            features: (ucp_feature::UCP_FEATURE_RMA
                | ucp_feature::UCP_FEATURE_TAG
                | ucp_feature::UCP_FEATURE_STREAM
                | ucp_feature::UCP_FEATURE_AM)
                .0 as u64,
            mt_workers_shared: 1,
            ..unsafe { params_default.assume_init() }
        };

        let mut config = MaybeUninit::uninit();
        let status = ucp_config_read(std::ptr::null(), std::ptr::null(), config.as_mut_ptr());
        Error::from_status(status)?;
        let config = config.assume_init();

        let mut ctx: MaybeUninit<*mut ucx1_sys::ucp_context> = MaybeUninit::uninit();
        let status = ucp_init_version(1, 14, &params, config, ctx.as_mut_ptr());
        Error::from_status(status)?;
        let ctx = ctx.assume_init();
        Ok(Rc::new(Self { h: ctx }))
    }
}

struct Listener<S> {
    h: ucp_listener_h,
    _worker: Rc<Worker>,
    _conn_handler_data: Rc<ConnHandlerData<S>>,
}

impl<S> Drop for Listener<S> {
    fn drop(&mut self) {
        unsafe {
            trace!("drop listener");
            ucp_listener_destroy(self.h);
        }
    }
}

impl<S: Clone> Listener<S> {
    unsafe fn create(
        worker: &mut Rc<Worker>,
        addr: SocketAddr,
        cb: unsafe fn(ConnectionRequest, Rc<Worker>, S),
        state: S,
    ) -> Result<Rc<Listener<S>>, Error> {
        let listener_params_default = MaybeUninit::uninit();
        let sockaddr = socket2::SockAddr::from(addr);
        let sockaddr = ucs_sock_addr {
            addrlen: sockaddr.len(),
            addr: (&sockaddr.as_storage() as *const libc::sockaddr_storage)
                as *const ucx1_sys::sockaddr,
        };
        unsafe extern "C" fn callback<S: Clone>(
            conn_request: *mut ucx1_sys::ucp_conn_request,
            user_data: *mut c_void,
        ) {
            let user_data: Weak<ConnHandlerData<S>> = Weak::from_raw(user_data as _);
            let conn_request = ConnectionRequest::from_raw(conn_request);
            if let Some(user_data) = user_data.upgrade() {
                (user_data.cb)(
                    conn_request,
                    user_data.worker.clone(),
                    user_data.state.clone(),
                );
            }
        }

        let conn_handler_data = Rc::new(ConnHandlerData {
            cb,
            state: state.clone(),
            worker: worker.clone(),
        });
        trace!(
            weak = Rc::weak_count(worker),
            strong = Rc::strong_count(worker),
            "worker_in_conn_handler_data"
        );

        let conn_handler = ucx1_sys::ucp_listener_conn_handler {
            cb: Some(callback::<S>),
            arg: Rc::downgrade(&conn_handler_data).as_ptr() as _,
        };

        let listener_params = ucp_listener_params_t {
            field_mask: (ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_SOCK_ADDR
                | ucp_listener_params_field::UCP_LISTENER_PARAM_FIELD_CONN_HANDLER)
                .0 as u64,
            sockaddr,
            conn_handler,
            ..unsafe { listener_params_default.assume_init() }
        };

        let mut listener = MaybeUninit::uninit();

        let status = ucp_listener_create(worker.h, &listener_params, listener.as_mut_ptr());
        Error::from_status(status)?;
        let listener = listener.assume_init();
        trace!("listener_created");
        let listener = Listener {
            h: listener,
            _worker: worker.clone(),
            _conn_handler_data: conn_handler_data,
        };
        trace!(
            weak = Rc::weak_count(worker),
            strong = Rc::strong_count(worker),
            "worker_in_conn_handler_data2"
        );
        Ok(Rc::new(listener))
    }
}

struct ConnectionRequest {
    ptr: *mut ucp_conn_request,
}

impl ConnectionRequest {
    unsafe fn from_raw(ptr: *mut ucp_conn_request) -> Self {
        Self { ptr }
    }
}

impl Drop for ConnectionRequest {
    fn drop(&mut self) {
        unsafe { ucp_request_free(self.ptr as _) }
    }
}

// 基本はRcで保持する
// err_handlerが呼ばれると自動的にcloseされたとみなし、Weakを通してclosedフラグを立てる
// closedフラグが立っていない場合はdropでucp_ep_close_nbxする
// ucp_ep_destroyはdropで呼ぶ
struct Endpoint {
    ptr: ucp_ep_h,
    closed: Rc<RefCell<bool>>,
    worker: Rc<Worker>,
}

pub struct StatusPtr {
    ptr: ucs_status_ptr_t,
}

impl StatusPtr {
    unsafe fn wait(&self, worker: &Worker) -> Result<(), Error> {
        if !self.ptr.is_null() {
            Error::from_status(UCS_PTR_STATUS(self.ptr))?;
            let mut checked_status = ucs_status_t::UCS_INPROGRESS;
            while checked_status == ucs_status_t::UCS_INPROGRESS {
                checked_status = ucp_request_check_status(self.ptr);
                worker.progress();
            }
            Error::from_status(checked_status)
        } else {
            Ok(())
        }
    }
}

impl Endpoint {
    unsafe fn from_sockaddr(worker: Rc<Worker>, addr: SocketAddr) -> Result<Self, Error> {
        unsafe extern "C" fn err_handler(user_data: *mut c_void, _: ucp_ep_h, _: ucs_status_t) {
            let closed_flag: Weak<RefCell<bool>> = Weak::from_raw(user_data as _);
            if let Some(closed_flag) = closed_flag.upgrade() {
                *closed_flag.borrow_mut() = true;
            }
        }
        let ep_params_default = MaybeUninit::uninit();
        let closed_flag = Rc::new(RefCell::new(false));
        let sockaddr = socket2::SockAddr::from(addr);
        let sockaddr = ucs_sock_addr {
            addrlen: sockaddr.len(),
            addr: (&sockaddr.as_storage() as *const libc::sockaddr_storage)
                as *const ucx1_sys::sockaddr,
        };
        let ep_params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_SOCK_ADDR
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER)
                .0 as u64,
            err_mode: ucx1_sys::ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER,
            sockaddr,
            err_handler: ucp_err_handler {
                cb: Some(err_handler),
                arg: Rc::downgrade(&closed_flag).as_ptr() as _,
            },
            ..ep_params_default.assume_init()
        };
        let mut ep = MaybeUninit::uninit();
        let status = ucp_ep_create(worker.h, &ep_params, ep.as_mut_ptr());
        Error::from_status(status)?;
        trace!("endpoint_created_from_sockaddr");
        Ok(Self {
            ptr: ep.assume_init(),
            closed: closed_flag,
            worker,
        })
    }

    unsafe fn from_conn_req(
        worker: Rc<Worker>,
        conn_req: ConnectionRequest,
    ) -> Result<Self, Error> {
        unsafe extern "C" fn err_handler(user_data: *mut c_void, _: ucp_ep_h, _: ucs_status_t) {
            let closed_flag: Weak<RefCell<bool>> = Weak::from_raw(user_data as _);
            if let Some(closed_flag) = closed_flag.upgrade() {
                *closed_flag.borrow_mut() = true;
            }
        }
        let ep_params_default = MaybeUninit::uninit();
        let closed_flag = Rc::new(RefCell::new(false));
        let ep_params = ucp_ep_params {
            field_mask: (ucp_ep_params_field::UCP_EP_PARAM_FIELD_CONN_REQUEST
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE
                | ucp_ep_params_field::UCP_EP_PARAM_FIELD_ERR_HANDLER)
                .0 as u64,
            err_mode: ucx1_sys::ucp_err_handling_mode_t::UCP_ERR_HANDLING_MODE_PEER,
            err_handler: ucp_err_handler {
                cb: Some(err_handler),
                arg: Rc::downgrade(&closed_flag).as_ptr() as _,
            },
            conn_request: conn_req.ptr,
            ..ep_params_default.assume_init()
        };
        let mut ep = MaybeUninit::uninit();
        let status = ucp_ep_create(worker.h, &ep_params, ep.as_mut_ptr());
        Error::from_status(status)?;
        trace!("endpoint_created_from_conn_req");
        Ok(Self {
            ptr: ep.assume_init(),
            closed: closed_flag,
            worker,
        })
    }

    unsafe fn tag_send<B: AsRef<[u8]>, C: Fn(ucs_status_t)>(
        &self,
        tag: u64,
        buffer: B,
        callback: Weak<C>,
    ) -> StatusPtr {
        unsafe extern "C" fn cb<C: Fn(ucs_status_t)>(
            _: *mut c_void,
            status: ucs_status_t,
            user_data: *mut c_void,
        ) {
            let state: Weak<C> = Weak::from_raw(user_data as _);
            if let Some(callback) = state.upgrade() {
                (callback)(status)
            }
        }
        let params_default = MaybeUninit::uninit();
        let params = ucp_request_param_t {
            op_attr_mask: (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32
                | ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32),
            cb: ucp_request_param_t__bindgen_ty_1 {
                send: Some(cb::<C>),
            },
            user_data: callback.as_ptr() as _,
            ..params_default.assume_init()
        };
        let ptr = ucp_tag_send_nbx(
            self.ptr,
            buffer.as_ref().as_ptr() as _,
            buffer.as_ref().len(),
            tag,
            &params,
        );
        StatusPtr { ptr }
    }

    unsafe fn tag_recv<C: Fn(ucs_status_t)>(
        &self,
        buffer: &mut [MaybeUninit<u8>],
        tag: u64,
        #[allow(unused)] tag_mask: u64,
        callback: Weak<C>,
    ) -> StatusPtr {
        unsafe extern "C" fn cb<C: Fn(ucs_status_t)>(
            _: *mut c_void,
            status: ucs_status_t,
            _: *const ucp_tag_recv_info,
            user_data: *mut c_void,
        ) {
            let callback: Weak<C> = Weak::from_raw(user_data as _);
            if let Some(callback) = callback.upgrade() {
                (callback)(status)
            }
        }
        let params_default = MaybeUninit::uninit();
        let params = ucp_request_param_t {
            op_attr_mask: (ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32
                | ucp_op_attr_t::UCP_OP_ATTR_FIELD_USER_DATA as u32
                | ucp_op_attr_t::UCP_OP_ATTR_FIELD_DATATYPE as u32),
            cb: ucp_request_param_t__bindgen_ty_1 {
                recv: Some(cb::<C>),
            },
            user_data: callback.as_ptr() as _,
            ..params_default.assume_init()
        };
        let ptr = ucp_tag_send_nbx(
            self.ptr,
            buffer.as_ref().as_ptr() as _,
            buffer.as_ref().len(),
            tag,
            &params,
        );
        StatusPtr { ptr }
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        if !*self.closed.borrow() {
            unsafe {
                let req_params_default = MaybeUninit::uninit();
                let req_params = ucp_request_param_t {
                    op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_FLAGS as u32,
                    flags: ucp_ep_close_flags_t::UCP_EP_CLOSE_FLAG_FORCE.0,
                    ..req_params_default.assume_init()
                };
                let status = ucp_ep_close_nbx(self.ptr, &req_params);
                let status = StatusPtr { ptr: status };
                if let Err(e) = status.wait(&self.worker) {
                    error!("{e}");
                }
            }
        }
        unsafe { ucp_ep_destroy(self.ptr) }
    }
}

unsafe fn client_server_do_work(ep: Endpoint, is_server: bool) -> Result<(), Error> {
    if is_server {
        trace!("server_start");
        let mut buffer = [MaybeUninit::uninit(); 256];
        let rx_cb = Rc::new(|_| {});
        let tx_cb = Rc::new(|_| {});

        let status = ep.tag_recv(&mut buffer, 99, 0, Rc::downgrade(&rx_cb));
        status.wait(&ep.worker)?;
        let buffer: [u8; 256] = transmute(buffer);
        trace!(msg = String::from_utf8_lossy(&buffer).as_ref(), "recv");

        let status = ep.tag_send(99, &buffer, Rc::downgrade(&tx_cb));
        status.wait(&ep.worker)?;

        Ok(())
    } else {
        trace!("client_start");
        let rx_cb = Rc::new(|_| {});
        let tx_cb = Rc::new(|_| {});

        let status = ep.tag_send(99, "Hello World!".as_bytes(), Rc::downgrade(&tx_cb));
        status.wait(&ep.worker)?;

        let mut buffer = [MaybeUninit::uninit(); 256];
        let status = ep.tag_recv(&mut buffer, 99, 0, Rc::downgrade(&rx_cb));
        status.wait(&ep.worker)?;
        let buffer: [u8; 256] = transmute(buffer);

        trace!(msg = String::from_utf8_lossy(&buffer).as_ref(), "recv");

        Ok(())
    }
}

unsafe fn conn_handler(conn_req: ConnectionRequest, worker: Rc<Worker>, state: Rc<AtomicBool>) {
    trace!("incoming_request");
    let ep = match Endpoint::from_conn_req(worker, conn_req) {
        Ok(ep) => ep,
        Err(e) => {
            trace!("{e}");
            return;
        }
    };
    if let Err(e) = client_server_do_work(ep, true) {
        trace!("{e}");
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
    let port = 10032;

    if rank == 0 {
        trace!("trace_on");
        let child = world.process_at_rank(1);
        let ip = get_ip_candidates()?.pop().with_context(|| "ip not found")?;
        child.send_with_tag(ip.octets().as_ref(), 99);
        info!(ip = ip.to_string(), "send_ip");
        unsafe {
            let ctx = Context::init()?;
            let mut worker = Worker::create(&ctx)?;
            let end_flag = Rc::new(AtomicBool::new(false));
            if let Err(e) = Listener::create(
                &mut worker,
                SocketAddr::V4(SocketAddrV4::new(ip, port)),
                conn_handler,
                end_flag.clone(),
            ) {
                warn!("{e}");
            }
            trace!(
                weak = Rc::weak_count(&worker),
                strong = Rc::strong_count(&worker),
                "main"
            );
            trace!("server_created");
            while !end_flag.load(Ordering::Relaxed) {
                worker.progress();
            }
            trace!("{}", end_flag.load(Ordering::Relaxed));
        };
        trace!("server_exited");
    } else {
        trace!("trace_on");
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
        unsafe {
            let ctx = Context::init()?;\
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
            trace!("client_ep_created");
            if let Err(e) = client_server_do_work(ep, false) {
                warn!("{e}");
            }
        }
        trace!("client_exited");
    }
    Ok(())
}
