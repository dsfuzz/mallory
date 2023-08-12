use ifcfg::IfCfg;
use ipnetwork::IpNetwork;
use std::error::Error;

pub fn resolve_to_experiment_ip(ip_or_host: &str, exp_net: IpNetwork) -> String {
    let ips = match dns_lookup::lookup_host(ip_or_host) {
        Ok(ips) => ips,
        Err(e) => {
            log::error!(
                "[FIREWALL] Failed to resolve {} to an IP address: {}",
                ip_or_host,
                e
            );
            // FIXME: Is this the right thing to do?
            return ip_or_host.to_string();
        }
    };

    // Out of all the results, select the first one that is in the experiment network
    for ip in ips {
        if exp_net.contains(ip) {
            return ip.to_string();
        }
    }

    ip_or_host.to_string()
}

pub fn get_experiment_interfaces(exp_net: IpNetwork) -> Result<Vec<IfCfg>, Box<dyn Error>> {
    let ifaces = match ifcfg::IfCfg::get() {
        Ok(ifaces) => ifaces,
        Err(err) => Err(err.to_string())?,
    };

    // Identify interfaces which contain the IP addresses within the experiment network
    let exp_ifaces: Vec<IfCfg> = ifaces
        .into_iter()
        .filter(|iface| {
            iface.addresses.iter().any(|&addr| match addr.address {
                Some(a) => exp_net.contains(a.ip()),
                _ => false,
            })
        })
        .collect();

    Ok(exp_ifaces)
}

pub fn install_nfqueue(
    ifaces: Vec<IfCfg>,
    queue_num: u16,
    queue_max_len: u32,
) -> Result<nfq::Queue, Box<dyn Error>> {
    // TODO: better performance if we use one queue per interface?
    let is_ipv6 = false;
    let ipt = iptables::new(is_ipv6)?;

    for iface in ifaces {
        let iface_name = iface.name;
        // We install the NFQUEUE on the FORWARD chain because we assume
        // that the mediator host is already configured (via ip route)
        // to forward packets between the nodes.
        ipt.append_replace(
            "filter",
            "FORWARD",
            &format!("-i {iface_name} -j NFQUEUE --queue-num {queue_num}"),
        )?;
    }

    let mut queue = nfq::Queue::open()?;
    queue.bind(queue_num)?;
    queue.set_queue_max_len(queue_num, queue_max_len)?;
    queue.set_nonblocking(true);

    Ok(queue)
}
