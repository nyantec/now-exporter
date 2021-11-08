use tokio::io::{Error, Result, ErrorKind};
use tokio::net::UdpSocket;
use miniz_oxide::deflate::compress_to_vec;
use log::{debug, trace};
use serde::Serialize;

pub struct Exporter {
    udp_sock: UdpSocket,
    collector_address: String,
}

impl Exporter {
    pub async fn new(listen_address: String, collector_address: String) -> Result<Self> {
        let udp_sock = UdpSocket::bind(listen_address).await?;
        Ok(Self { udp_sock, collector_address })
    }

    pub async fn submit<T: Serialize>(&mut self, data: T) -> Result<()> {
        let uncompressed_data = serde_json::to_string(&data)?;
        trace!("{}", &uncompressed_data);
        let compressed_data = compress_to_vec(uncompressed_data.as_bytes(), 4);
        debug!("sending packet. uncompressed size: {} compressed size: {}", uncompressed_data.len(), compressed_data.len());
        if compressed_data.len() < 65536 {
            let sent_len = self.udp_sock.send_to(&compressed_data, &self.collector_address).await?;
            if sent_len != compressed_data.len() {
                return Err(Error::new(ErrorKind::Other, format!("packet was truncated. this shouldn't happen. sent_len={}, olen={}", sent_len, compressed_data.len())));
            }
        } else {
            return Err(Error::new(ErrorKind::Other, format!("compressed data does not fit in 65536 bytes")));
        }
        Ok(())
    }
}
