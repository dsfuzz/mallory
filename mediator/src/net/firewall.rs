extern crate rocket;
use dashmap::DashMap;
use ipnetwork::IpNetwork;
use rocket::serde::Serialize;
use std::fmt;

use crate::net;
use crate::net::packet::{Packet, PacketIdentifier};

#[derive(PartialOrd, Ord, PartialEq, Eq, Default, Debug)]
pub enum FirewallVerdict {
    Accept,
    Drop,
    #[default]
    NotDecided,
}

impl fmt::Display for FirewallVerdict {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FirewallVerdict::Accept => write!(f, "Accept"),
            FirewallVerdict::Drop => write!(f, "Drop"),
            FirewallVerdict::NotDecided => write!(f, "NotDecided"),
        }
    }
}

/// Firewall used by the mediator to filter packets.
#[derive(Serialize)]
pub struct Firewall {
    /// key = SRC, value = list of DST
    drop_rules: DashMap<String, Vec<String>>,
    unfiltered_ports: Vec<u16>,

    experiment_network: IpNetwork,
}

impl Firewall {
    pub fn new(unfiltered_ports: Vec<u16>, experiment_network: IpNetwork) -> Firewall {
        Firewall {
            drop_rules: DashMap::new(),
            unfiltered_ports,
            experiment_network,
        }
    }

    pub fn statistics(&self) -> String {
        let mut rules: Vec<(String, String)> = self
            .drop_rules
            .iter()
            .flat_map(|kv| {
                let src = kv.key();
                let dsts = kv.value();
                dsts.iter()
                    .map(|dst| (src.to_string(), dst.to_string()))
                    .collect::<Vec<_>>()
            })
            .collect();
        rules.sort_unstable();
        let num_rules = rules.len();
        format!("{} firewall rules are in effect: {:?} ", num_rules, rules)
    }

    /* Helper functions */
    fn firewall_decision(&self, pkt: &Packet) -> bool {
        let si = pkt.packet_identifier();
        match si {
            // (1) If we know how to classify this, filter it through the firewall
            PacketIdentifier {
                src_ip: Some(src_ip),
                dst_ip: Some(dst_ip),
                src_port: Some(sport),
                dst_port: Some(dport),
                ..
            } => {
                // (a) Accept if this packet is on an unfiltered port
                let unfiltered_ports = self.unfiltered_ports.as_slice();
                if unfiltered_ports.iter().any(|&p| p == sport || p == dport) {
                    return true;
                }

                // (b) If it's a filtered port, drop packets according to drop rules
                let (src, dst) = (src_ip.to_string(), dst_ip.to_string());
                if let Some(dsts) = self.drop_rules.get(&src) {
                    if dsts.iter().any(|d| d.eq(&dst)) {
                        return false;
                    }
                }

                // By default, we accept
                true
            }
            // (2) Accept everything we cannot classify / we're not familiar with
            _ => true,
        }
    }

    pub fn firewall_set_verdict(&self, pkt: &mut Packet) {
        let decision = self.firewall_decision(pkt);
        match decision {
            true => pkt.set_verdict(FirewallVerdict::Accept),
            false => pkt.set_verdict(FirewallVerdict::Drop),
        }
    }

    /// Add a drop rule to the firewall
    pub fn drop(&self, src: String, dst: String) {
        let src = net::util::resolve_to_experiment_ip(&src, self.experiment_network);
        let dst = net::util::resolve_to_experiment_ip(&dst, self.experiment_network);

        if let Some(mut dsts) = self.drop_rules.get_mut(&src) {
            if !dsts.iter().any(|d| d.eq(&dst)) {
                dsts.push(dst);
            }
        } else {
            self.drop_rules.insert(src, vec![dst]);
        }
    }

    /// Clear all drop rules
    pub fn heal(&self) {
        self.drop_rules.clear();
    }
}
