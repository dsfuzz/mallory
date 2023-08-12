// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::tikv_client_server::error::Error as ClientServerError;
use super::tikv_client_server::raw::client_server::Client;
use super::tikv_client_server::raw::{GetReply, GetRequest, PutReply, PutRequest};
use log::info;
use std::str;
use tikv_client::RawClient;
use tikv_client_common::Error;
use tonic::{Request, Response, Status};

pub struct ClientProxy {
    client: RawClient,
}

impl ClientProxy {
    pub fn new(client: RawClient) -> ClientProxy {
        ClientProxy { client }
    }
}

#[tonic::async_trait]
impl Client for ClientProxy {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetReply>, Status> {
        info!("Got a request: {:?}", request);
        let response = match self.client.get(request.into_inner().key).await {
            Ok(response) => response,
            Err(err) => {
                match err {
                    // TODO(ziyi) clarify whether each error is determined or not.
                    Error::UndeterminedError(_) | Error::Grpc(_) => {
                        return Ok(Response::new(GetReply {
                            value: "".to_owned(),
                            error: Some(ClientServerError {
                                undetermined: format!("tikv client get() failed: {:?}", err),
                                not_found: "".to_owned(),
                                aborted: "".to_owned(),
                            }),
                        }));
                    }
                    _ => {
                        return Ok(Response::new(GetReply {
                            value: "".to_owned(),
                            error: Some(ClientServerError {
                                aborted: format!("tikv client get() aborted: {:?}", err),
                                not_found: "".to_owned(),
                                undetermined: "".to_owned(),
                            }),
                        }));
                    }
                };
            }
        };
        match response {
            Some(value) => Ok(Response::new(GetReply {
                value: str::from_utf8(value.as_ref()).unwrap().into(),
                error: None,
            })),
            None => Ok(Response::new(GetReply {
                value: "".to_owned(),
                error: Some(ClientServerError {
                    not_found: format!("key is not found"),
                    undetermined: "".to_owned(),
                    aborted: "".to_owned(),
                }),
            })),
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutReply>, Status> {
        info!("Got a request: {:?}", request);
        let message = request.into_inner();
        match self.client.put(message.key, message.value).await {
            Ok(()) => Ok(Response::new(PutReply { error: None })),
            Err(err) => match err {
                Error::UndeterminedError(_) | Error::Grpc(_) => {
                    return Ok(Response::new(PutReply {
                        error: Some(ClientServerError {
                            undetermined: format!("tikv client put() failed: {:?}", err),
                            not_found: "".to_owned(),
                            aborted: "".to_owned(),
                        }),
                    }));
                }
                _ => {
                    return Ok(Response::new(PutReply {
                        error: Some(ClientServerError {
                            aborted: format!("tikv client put() aborted: {:?}", err),
                            undetermined: "".to_owned(),
                            not_found: "".to_owned(),
                        }),
                    }))
                }
            },
        }
    }
}
