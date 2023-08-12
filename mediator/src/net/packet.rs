/* Data structures and methods for manipulating packets. */

use chrono::{DateTime, Utc};
use std::fmt;

use crate::history::common::NodeIp;
use crate::history::time::ClockManager;
pub use crate::net::firewall::FirewallVerdict;
pub use crate::net::raw_packet::PacketIdentifier;
use crate::net::raw_packet::RawPacket;

pub type PacketUid = usize;
pub type NetworkId = u64;

// #[derive(Debug)]
pub struct Packet {
    /// Index for the packet. This is a truly unique ID assigned by the mediator.
    pub mediator_id: PacketUid,

    /// Network ID. This is an ID determined from the packet itself, and may repeat
    /// over long periods of time. This is the ID that Packet events are tagged with
    /// by the coverage-server at the nodes.
    pub network_id: NetworkId,

    /// Set to None after the verdict is issued.
    pub msg: Option<nfq::Message>,

    packet_identifier: PacketIdentifier,
    abstract_value: u32,

    #[cfg(feature = "logsaving")]
    /// The original length of the packet, as reported by the kernel.
    original_len: usize,

    #[cfg(feature = "logsaving")]
    /// The payload in msg disappears after the verdict is issued, so we store it separately.
    pkt: RawPacket,

    /// Firewall decision.
    pub verdict: FirewallVerdict,
    /// Timestamps for when we've received and emitted the packet.
    pub received: DateTime<Utc>,
    pub emitted: Option<DateTime<Utc>>,
}

impl Packet {
    pub fn new(msg: nfq::Message, mediator_id: PacketUid) -> Self {
        // let ts: DateTime<Utc> = match msg.get_timestamp() {
        //     Some(ts) => chrono::DateTime::from(ts),
        //     None => ClockManager::utc_now(),
        // };
        // We use our own timestamp rather than the kernel's,
        // because the kernel's timestamp seems to sometimes be wrong??
        let ts = ClockManager::utc_now();

        let payload = msg.get_payload().to_owned();
        let original_len = msg.get_original_len();

        let raw_pkt = RawPacket::new(payload);
        let (packet_identifier, transport_payload) = raw_pkt.parse_packet();
        let abstract_value = Self::first_4_bytes(&transport_payload);
        let network_id = packet_identifier.network_id();

        Self {
            mediator_id,
            network_id,
            msg: Some(msg),
            packet_identifier,
            abstract_value,
            #[cfg(feature = "logsaving")]
            original_len,
            #[cfg(feature = "logsaving")]
            pkt: raw_pkt,
            verdict: FirewallVerdict::NotDecided,
            received: ts,
            emitted: None,
        }
    }

    fn first_4_bytes(payload: &[u8]) -> u32 {
        u32::from_le_bytes([
            *payload.get(0).unwrap_or(&0),
            *payload.get(1).unwrap_or(&0),
            *payload.get(2).unwrap_or(&0),
            *payload.get(3).unwrap_or(&0),
        ])
    }

    /// An abstraction of this packet's value for use in histories.
    // TODO: Figure out what's a reasonable abstraction based on experiments.
    pub fn abstract_value(&self) -> u32 {
        self.abstract_value
    }

    /// `NotDecided` returns false.
    pub fn was_certainly_dropped(&self) -> bool {
        self.verdict == FirewallVerdict::Drop
    }

    pub fn mark_emitted(&mut self) {
        self.msg = None;
        self.emitted = Some(ClockManager::utc_now());
    }

    /// Helper for generating PCAP files.
    #[cfg(feature = "logsaving")]
    pub fn get_recv_pcap_tuple(&self) -> (u32, u32, &[u8], u32) {
        let ts_sec = self.received.timestamp();
        let ts_nsec = self.received.timestamp_subsec_nanos();
        let ip_data = self.pkt.packet.as_slice();
        let ip_len = self.original_len;
        (ts_sec as u32, ts_nsec as u32, ip_data, ip_len as u32)
    }

    #[cfg(feature = "logsaving")]
    pub fn get_send_pcap_tuple(&self) -> (u32, u32, &[u8], u32) {
        assert!(
            self.verdict != FirewallVerdict::NotDecided && self.emitted.is_some(),
            "Cannot get send PCAP tuple for a packet that has not been emitted."
        );
        let ts_sec = self.emitted.unwrap().timestamp();
        let ts_nsec = self.emitted.unwrap().timestamp_subsec_nanos();
        let ip_data = self.pkt.packet.as_slice();
        let ip_len = self.original_len;
        (ts_sec as u32, ts_nsec, ip_data, ip_len as u32)
    }

    pub fn set_verdict(&mut self, verdict: FirewallVerdict) {
        assert!(
            self.verdict == FirewallVerdict::NotDecided,
            "Trying to set a verdict twice for {}!",
            self
        );
        match &mut self.msg {
            None => assert!(
                self.msg.is_none(),
                "Trying to set verdict for a packet with no nfqueue `msg`!"
            ),
            Some(msg) => {
                match verdict {
                    FirewallVerdict::Accept => msg.set_verdict(nfq::Verdict::Accept),
                    FirewallVerdict::Drop => msg.set_verdict(nfq::Verdict::Drop),
                    FirewallVerdict::NotDecided => panic!("Trying to set verdict to NotDecided!"),
                }
                self.verdict = verdict;
            }
        }
    }

    pub fn has_been_dropped(&self) -> bool {
        self.verdict == FirewallVerdict::Drop
    }

    pub fn get_sender_receiver_ips(&self) -> (Option<NodeIp>, Option<NodeIp>) {
        let ident = self.packet_identifier();
        let src_str = ident.src_ip.map(|src| src.to_string());
        let dst_str = ident.dst_ip.map(|dst| dst.to_string());
        (src_str, dst_str)
    }

    pub fn packet_identifier(&self) -> PacketIdentifier {
        self.packet_identifier.clone()
    }
}

impl fmt::Display for Packet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "({}) [UID {}] {}",
            self.verdict,
            self.mediator_id,
            self.packet_identifier(),
        )
    }
}
