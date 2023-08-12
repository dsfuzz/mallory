use std::error::Error;

pub fn install_nfqueue(
    host_ip: String,
    mediator_ip: String,
    queue_num: u16,
    queue_max_len: u32,
) -> Result<nfq::Queue, Box<dyn Error>> {
    let is_ipv6 = false;
    let ipt = iptables::new(is_ipv6)?;

    // Don't filter loopback
    ipt.append_replace("filter", "INPUT", "-i lo -j ACCEPT")?;

    // Don't filter packets to ourselves (i.e. this host -> this host)
    ipt.append_replace(
        "filter",
        "INPUT",
        &format!("-s {} -d {} -j ACCEPT", host_ip, host_ip),
    )?;
    ipt.append_replace(
        "filter",
        "OUTPUT",
        &format!("-s {} -d {} -j ACCEPT", host_ip, host_ip),
    )?;

    // We want to immediately accept all packets to/from the mediator
    ipt.append_replace("filter", "INPUT", &format!("-s {} -j ACCEPT", mediator_ip))?;
    ipt.append_replace("filter", "OUTPUT", &format!("-d {} -j ACCEPT", mediator_ip))?;

    // --queue-bypass means that if cov-server is not running, the packets will be accepted
    ipt.append_replace(
        "filter",
        "INPUT",
        &format!("-j NFQUEUE --queue-num {queue_num} --queue-bypass"),
    )?;
    ipt.append_replace(
        "filter",
        "OUTPUT",
        &format!("-j NFQUEUE --queue-num {queue_num} --queue-bypass"),
    )?;

    let mut queue = nfq::Queue::open()?;
    queue.bind(queue_num)?;
    queue.set_queue_max_len(queue_num, queue_max_len)?;
    queue.set_nonblocking(false);
    // If the queue is full, accept, so the application sees it
    queue.set_fail_open(queue_num, true)?;

    Ok(queue)
}
