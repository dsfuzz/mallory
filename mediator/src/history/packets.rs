use std::error::Error;
use std::path::Path;
use std::time::Instant;

use antidote::RwLock;
use dashmap::DashMap;
use dashmap::DashSet;
use pcap_file::pcap::Packet as PcapPacket;
use pcap_file::pcap::PcapHeader;
use pcap_file::PcapWriter;

use crate::net::firewall::FirewallVerdict;
use crate::net::packet::NetworkId;
use crate::net::packet::Packet;
use crate::net::packet::PacketUid;

use super::common::NodeIp;

/// NOTE: this is not the same as PacketUid. It is an index into the `packets` vector.
type PacketIndex = usize;
type AbstractData = u32;
type DropStatus = bool;

// Magic header for writing PCAP files.
const PCAP_HEADER: PcapHeader = PcapHeader {
    magic_number: 0xa1b2c3d4,
    version_major: 2,
    version_minor: 4,
    ts_correction: 0,
    ts_accuracy: 0,
    snaplen: 65535,
    // This is the important bit, saying that the captured packets are RAW IP (v4 or v6).
    datalink: pcap_file::DataLink::RAW,
};

/// Keeps track of packets seen by the mediator. Supports dumping to a PCAP
/// file and interrogation of the history (e.g. to add events to a timeline).
pub struct Packets {
    /// Used to generate PCAP files. Packets are pushed once they are emitted
    /// by the mediator.
    packets: RwLock<Vec<Packet>>,

    /// Used by the feedback mechanism to convert network IDs to mediator IDs.
    /// Packets are added here when _received_ by the mediator, i.e. before
    /// they are added to `packets`.
    packets_by_network_id: DashMap<
        NetworkId,
        (
            PacketUid,
            NodeIp, // sender
            NodeIp, // recipient
            AbstractData,
            DropStatus,
            Option<PacketIndex>,
        ),
    >,

    #[cfg(feature = "selfcheck")]
    /// We check whether packets we do not recognize when submitted in a batch
    /// arrive later at the mediator.
    _not_recognized: DashSet<NetworkId>,
}

impl Packets {
    pub fn new() -> Self {
        Self {
            packets: RwLock::new(Vec::new()),
            packets_by_network_id: DashMap::new(),
            #[cfg(feature = "selfcheck")]
            _not_recognized: DashSet::new(),
        }
    }

    pub fn received_packet(&self, packet: &Packet) {
        if self.packets_by_network_id.get(&packet.network_id).is_some() {
            log::debug!(
                "[PACKETS] Received packet with duplicate network ID: {}. Overwriting old entry.",
                packet.network_id,
            );
        }
        #[cfg(feature = "selfcheck")]
        if self._not_recognized.contains(&packet.network_id) {
            // See the [batch_before_packet] comment in `parse_and_add_instrumented events.
            // We should implement a proper solution.
            log::info!(
                "[SELFCHECK] Received late packet with network ID {} that was (previously) not recognized in a batch: {:?}",
                packet.network_id, packet.pkt,
            );
        }
        let (src_ip, dst_ip) = packet.get_sender_receiver_ips();
        self.packets_by_network_id.insert(
            packet.network_id,
            (
                packet.mediator_id,
                src_ip.expect("Packet has no sender IP"),
                dst_ip.expect("Packet has no recipient IP"),
                packet.abstract_value(),
                packet.was_certainly_dropped(),
                None,
            ),
        );
    }

    pub fn emitted_packet(&self, packet: Packet) {
        let mut packets = self.packets.write();
        let pkt_idx: PacketIndex = packets.len();
        let (src_ip, dst_ip) = packet.get_sender_receiver_ips();
        self.packets_by_network_id.insert(
            packet.network_id,
            (
                packet.mediator_id,
                src_ip.expect("Packet has no sender IP"),
                dst_ip.expect("Packet has no recipient IP"),
                packet.abstract_value(),
                packet.was_certainly_dropped(),
                Some(pkt_idx),
            ),
        );
        packets.push(packet);
    }

    pub fn get_mediator_id(&self, network_id: NetworkId) -> Option<PacketUid> {
        if let Some(pkt_ref) = self.packets_by_network_id.get(&network_id) {
            let (mediator_id, _, _, _, _, _) = pkt_ref.value();
            Some(*mediator_id)
        } else {
            // Record that we did not recognise this packet.
            #[cfg(feature = "selfcheck")]
            self._not_recognized.insert(network_id);
            None
        }
    }

    pub fn get_abstract_data(&self, network_id: NetworkId) -> Option<AbstractData> {
        if let Some(pkt_ref) = self.packets_by_network_id.get(&network_id) {
            let (_, _, _, abs_data, _, _) = pkt_ref.value();
            Some(*abs_data)
        } else {
            None
        }
    }

    pub fn was_certainly_dropped(&self, network_id: NetworkId) -> DropStatus {
        if let Some(pkt_ref) = self.packets_by_network_id.get(&network_id) {
            let (_, _, _, _, drop_status, _) = pkt_ref.value();
            *drop_status
        } else {
            false
        }
    }

    pub fn get_sender_receiver(&self, network_id: NetworkId) -> Option<(NodeIp, NodeIp)> {
        if let Some(pkt_ref) = self.packets_by_network_id.get(&network_id) {
            let (_, src_ip, dst_ip, _, _, _) = pkt_ref.value();
            Some((src_ip.clone(), dst_ip.clone()))
        } else {
            None
        }
    }

    /// This function is unusable for now because it is too slow.
    pub fn dump(&self, base_path: &Path) -> Result<(), Box<dyn Error>> {
        #[cfg(feature = "logsaving")]
        {
            let start = Instant::now();

            // We don't write to the end file directly because that may be on a network volume,
            // which for some reason makes the performance of this function terrible (~60s for ~200k packets).
            let tmp_transmit_file = tempfile::NamedTempFile::new()?;
            let tmp_drop_file = tempfile::NamedTempFile::new()?;

            let mut transmit_writer = PcapWriter::with_header(PCAP_HEADER, tmp_transmit_file)?;
            let mut drop_writer = PcapWriter::with_header(PCAP_HEADER, tmp_drop_file)?;

            let packets = self.packets.read();

            for pkt in packets.iter() {
                let (ts_sec, ts_nsec, data, orig_len) = pkt.get_send_pcap_tuple();
                let snd_pkt = PcapPacket::new(ts_sec, ts_nsec, data, orig_len);

                match pkt.verdict {
                    FirewallVerdict::Accept => transmit_writer.write_packet(&snd_pkt)?,
                    FirewallVerdict::Drop => drop_writer.write_packet(&snd_pkt)?,
                    FirewallVerdict::NotDecided => {
                        log::warn!("Packet {} did not receive a firewall decision!", pkt)
                    }
                }
            }

            let _write_duration = start.elapsed();
            let _wrote = Instant::now();

            let transmit_file_path = base_path.join("transmit.pcap");
            let drop_file = base_path.join("drop.pcap");

            std::fs::copy(
                transmit_writer.into_writer().into_temp_path(),
                transmit_file_path,
            )?;
            std::fs::copy(drop_writer.into_writer().into_temp_path(), drop_file)?;

            log::info!(
                "[PERF] Dumped {} packets to PCAP files in {} ms (writing took {} ms / copying took {} ms).",
                packets.len(),
                start.elapsed().as_millis(),
                _write_duration.as_millis(),
                _wrote.elapsed().as_millis()
            );
        }

        Ok(())
    }
}
