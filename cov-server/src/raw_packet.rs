// This file is shared with the coverage-server and must be kept in sync
// across both projects for packet events to be processed correctly by
// the mediator.

use std::net::IpAddr;
use std::{
    fmt,
    hash::{Hash, Hasher},
};
extern crate siphasher;
use pnet::packet::ethernet::{EtherTypes, EthernetPacket};
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::packet::ipv4::Ipv4Packet;
use pnet::packet::ipv6::Ipv6Packet;
use pnet::packet::{icmp, icmpv6, ipv4, ipv6, tcp, udp, Packet};
use siphasher::sip::SipHasher;

#[derive(PartialOrd, Ord, PartialEq, Eq, Default, Hash, Debug, Clone)]
pub enum PacketType {
    Tcp,
    Udp,
    Icmpv4,
    Icmpv6,
    CouldNotParse,
    #[default]
    Unknown,
}

impl fmt::Display for PacketType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PacketType::Tcp => write!(f, "TCP"),
            PacketType::Udp => write!(f, "UDP"),
            PacketType::Icmpv4 => write!(f, "ICMPv4"),
            PacketType::Icmpv6 => write!(f, "ICMPv6"),
            PacketType::CouldNotParse => write!(f, "CouldNotParse"),
            PacketType::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Default, Hash, Debug, Clone)]
#[repr(C)]
pub struct PacketIdentifier {
    pub src_ip: Option<IpAddr>,
    pub dst_ip: Option<IpAddr>,
    pub src_port: Option<u16>,
    pub dst_port: Option<u16>,

    pub packet_type: PacketType,
    pub ip_ident: u32,        // Ipv4 identification field or Ipv6 flow label
    pub transport_ident: u32, // TCP seqnum or UDP checksum
}

impl PacketIdentifier {
    pub fn network_id(&self) -> u64 {
        let mut hasher = SipHasher::new_with_keys(0, 0);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for PacketIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let network_id = self.network_id();
        match self {
            PacketIdentifier {
                src_ip: Some(src_ip),
                dst_ip: Some(dst_ip),
                src_port: Some(src_port),
                dst_port: Some(dst_port),
                packet_type,
                ..
            } => write!(
                f,
                "{} {}:{} -> {}:{} (network_id: {})",
                packet_type, src_ip, src_port, dst_ip, dst_port, network_id
            ),
            PacketIdentifier {
                src_ip: Some(src_ip),
                dst_ip: Some(dst_ip),
                src_port: None,
                dst_port: None,
                packet_type,
                ..
            } => write!(
                f,
                "{} {} -> {} (network_id: {})",
                packet_type, src_ip, dst_ip, network_id
            ),
            PacketIdentifier { packet_type, .. } => {
                write!(f, "{} (network_id: {})", packet_type, network_id)
            }
        }
    }
}

#[derive(Debug)]
pub struct RawPacket {
    pub packet: Vec<u8>,
}

impl RawPacket {
    pub fn new(packet: Vec<u8>) -> Self {
        Self { packet }
    }

    #[allow(dead_code)]
    fn parse_ethernet(&self) -> (Option<Ipv4Packet>, Option<Ipv6Packet>) {
        if let Some(eth_pkt) = EthernetPacket::new(&self.packet) {
            let eth_payload = eth_pkt.payload().to_vec();
            match eth_pkt.get_ethertype() {
                EtherTypes::Ipv4 => (ipv4::Ipv4Packet::owned(eth_payload), None),
                EtherTypes::Ipv6 => (None, ipv6::Ipv6Packet::owned(eth_payload)),
                _ => (None, None),
            }
        } else {
            (None, None)
        }
    }

    fn parse_ip(&self) -> (Option<Ipv4Packet>, Option<Ipv6Packet>) {
        (
            ipv4::Ipv4Packet::new(&self.packet),
            ipv6::Ipv6Packet::new(&self.packet),
        )
    }

    pub fn parse_packet(&self) -> (PacketIdentifier, Vec<u8>) {
        let mut si = PacketIdentifier {
            packet_type: PacketType::CouldNotParse,
            ..Default::default()
        };

        let (ipv4, ipv6) = self.parse_ip();
        
        // Set the IP header fields in the PacketIdentifier.
        let (ip_payload, next_proto, src_ip, dst_ip, ip_ident) = if let Some(ipv4) = ipv4 {
            (
                ipv4.payload().to_vec(),
                ipv4.get_next_level_protocol(),
                IpAddr::V4(ipv4.get_source()),
                IpAddr::V4(ipv4.get_destination()),
                ipv4.get_identification() as u32,
            )
        } else if let Some(ipv6) = ipv6 {
            (
                ipv6.payload().to_vec(),
                ipv6.get_next_header(),
                IpAddr::V6(ipv6.get_source()),
                IpAddr::V6(ipv6.get_destination()),
                ipv6.get_flow_label(),
            )
        } else {
            // If neither Ipv4 or Ipv6, we give up with `CouldNotParse`.
            return (si, vec![]);
        };

        si.src_ip = Some(src_ip);
        si.dst_ip = Some(dst_ip);
        si.ip_ident = ip_ident;

        let (transport_payload, pkt_type, src_port, dst_port, transport_ident) = {
            match next_proto {
                IpNextHeaderProtocols::Tcp => {
                    if let Some(tcp) = tcp::TcpPacket::new(&ip_payload) {
                        (
                            tcp.payload().to_vec(),
                            PacketType::Tcp,
                            Some(tcp.get_source()),
                            Some(tcp.get_destination()),
                            tcp.get_sequence(),
                        )
                    } else {
                        (vec![], PacketType::CouldNotParse, None, None, 0)
                    }
                }
                IpNextHeaderProtocols::Udp => {
                    if let Some(udp) = udp::UdpPacket::new(&ip_payload) {
                        (
                            udp.payload().to_vec(),
                            PacketType::Udp,
                            Some(udp.get_source()),
                            Some(udp.get_destination()),
                            udp.get_checksum() as u32,
                        )
                    } else {
                        (vec![], PacketType::CouldNotParse, None, None, 0)
                    }
                }
                IpNextHeaderProtocols::Icmp => {
                    if let Some(icmp) = icmp::IcmpPacket::new(&ip_payload) {
                        (
                            icmp.payload().to_vec(),
                            PacketType::Icmpv4,
                            None,
                            None,
                            icmp.get_checksum() as u32,
                        )
                    } else {
                        (vec![], PacketType::CouldNotParse, None, None, 0)
                    }
                }
                IpNextHeaderProtocols::Icmpv6 => {
                    if let Some(icmpv6) = icmpv6::Icmpv6Packet::new(&ip_payload) {
                        (
                            icmpv6.payload().to_vec(),
                            PacketType::Icmpv6,
                            None,
                            None,
                            icmpv6.get_checksum() as u32,
                        )
                    } else {
                        (vec![], PacketType::CouldNotParse, None, None, 0)
                    }
                }
                _ => (vec![], PacketType::CouldNotParse, None, None, 0),
            }
        };
        si.packet_type = pkt_type;
        si.src_port = src_port;
        si.dst_port = dst_port;
        si.transport_ident = transport_ident;

        (si, transport_payload)
    }

    #[allow(dead_code)]
    fn first_4_bytes(payload: &[u8]) -> u32 {
        u32::from_le_bytes([
            *payload.get(0).unwrap_or(&0),
            *payload.get(1).unwrap_or(&0),
            *payload.get(2).unwrap_or(&0),
            *payload.get(3).unwrap_or(&0),
        ])
    }
}
