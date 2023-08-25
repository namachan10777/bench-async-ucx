use std::net::{IpAddr, Ipv4Addr};

use network_interface::{NetworkInterfaceConfig, NetworkInterface};
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
