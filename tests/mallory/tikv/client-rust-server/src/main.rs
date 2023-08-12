// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use log::{error, info, LevelFilter};
use simple_logging;

use clap::{App, Arg};
use tonic::transport::Server;

use tikv_client::{RawClient, TransactionClient};

use tikv_client_server::raw::client_server::ClientServer as RawClientServer;
use tikv_client_server::txn::client_server::ClientServer as TxnClientServer;

use raw::ClientProxy as RawClientProxy;
use txn::ClientProxy as TxnClientProxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logging::log_to_stderr(LevelFilter::Info);

    let matches = App::new("client-rust server")
        .version("0.1")
        .author("Ziyi Yan <ziyi.yan@foxmail.com>")
        .about("A server to proxy tikv operations built on client-rust")
        .arg(
            Arg::with_name("node")
                .long("node")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .long("port")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("type")
                .long("type")
                .required(true)
                .takes_value(true)
                .possible_values(&["raw", "txn"]),
        )
        .get_matches();

    let node = matches.value_of("node").unwrap();
    let typ = matches.value_of("type").unwrap();
    let port = matches.value_of("port").unwrap();
    let addr = format!("127.0.0.1:{}", port).parse()?;
    info!("{}", addr);

    let pd_endpoints = vec![format!("{}:2379", node)];
    match typ {
        "raw" => {
            let client = RawClient::new(pd_endpoints).await?;
            let proxy = RawClientProxy::new(client);
            let server = RawClientServer::new(proxy);
            Server::builder().add_service(server).serve(addr).await?;
        }
        "txn" => {
            let client = TransactionClient::new(pd_endpoints).await?;
            let proxy = TxnClientProxy::new(client);
            let server = TxnClientServer::new(proxy);
            Server::builder().add_service(server).serve(addr).await?;
        }
        _ => {
            error!("type is not one of \"raw\" and \"txn\"");
            std::process::exit(1);
        }
    };

    Ok(())
}

mod tikv_client_server;

mod raw;
mod txn;
