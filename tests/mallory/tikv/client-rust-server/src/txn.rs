// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use log::info;
use std::collections::BTreeMap;
use std::str;
use std::sync::atomic::{AtomicU32, Ordering};

use super::tikv_client_server::error::Error as ClientServerError;
use super::tikv_client_server::txn::begin_txn_request::Type;
use super::tikv_client_server::txn::client_server::Client;
use super::tikv_client_server::txn::{
    BeginTxnReply, BeginTxnRequest, CommitReply, CommitRequest, GetReply, GetRequest, PutReply,
    PutRequest, RollbackReply, RollbackRequest,
};
use tikv_client::{Transaction, TransactionClient};
use tikv_client_common::Error;
use tokio::sync::Mutex;
use tonic::Response;

pub struct ClientProxy {
    client: TransactionClient,
    txns: Mutex<BTreeMap<u32, Transaction>>,
    next_txn_id: AtomicU32,
}

impl ClientProxy {
    pub fn new(client: TransactionClient) -> ClientProxy {
        ClientProxy {
            client,
            txns: Mutex::new(BTreeMap::new()),
            next_txn_id: AtomicU32::new(1),
        }
    }
}

#[tonic::async_trait]
impl Client for ClientProxy {
    async fn begin_txn(
        &self,
        request: tonic::Request<BeginTxnRequest>,
    ) -> Result<tonic::Response<BeginTxnReply>, tonic::Status> {
        let req = request.into_inner();
        let res = match req.r#type() {
            Type::Optimistic => self.client.begin_optimistic().await,
            Type::Pessimistic => self.client.begin_pessimistic().await,
        };
        let txn = match res {
            Ok(txn) => txn,
            Err(err) => match err {
                Error::UndeterminedError(_) | Error::Grpc(_) => {
                    return Ok(Response::new(BeginTxnReply {
                        txn_id: 0,
                        error: Some(ClientServerError {
                            undetermined: format!("begin transaction failed: {:?}", err),
                            not_found: "".to_owned(),
                            aborted: "".to_owned(),
                        }),
                    }))
                }
                _ => {
                    return Ok(Response::new(BeginTxnReply {
                        txn_id: 0,
                        error: Some(ClientServerError {
                            aborted: format!("begin transaction aborted: {:?}", err),
                            not_found: "".to_owned(),
                            undetermined: "".to_owned(),
                        }),
                    }))
                }
            },
        };
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        self.txns.lock().await.insert(txn_id, txn);
        info!("txn: {} type: {:?} begin_txn()", txn_id, req.r#type());
        Ok(Response::new(BeginTxnReply {
            txn_id,
            error: None,
        }))
    }

    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetReply>, tonic::Status> {
        let GetRequest { key, txn_id } = request.into_inner();
        let mut txns = self.txns.lock().await;
        let txn = txns.get_mut(&txn_id).unwrap();
        let val = match txn.get(key.clone()).await {
            Ok(val) => val,
            Err(err) => match err {
                Error::UndeterminedError(_) | Error::Grpc(_) => {
                    return Ok(Response::new(GetReply {
                        value: "".to_owned(),
                        error: Some(ClientServerError {
                            undetermined: format!("tikv transaction get failed: {:?}", err),
                            not_found: "".to_owned(),
                            aborted: "".to_owned(),
                        }),
                    }))
                }
                _ => {
                    return Ok(Response::new(GetReply {
                        value: "".to_owned(),
                        error: Some(ClientServerError {
                            aborted: format!("tikv transaction get aborted: {:?}", err),
                            undetermined: "".to_owned(),
                            not_found: "".to_owned(),
                        }),
                    }))
                }
            },
        };
        let res = match val {
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
        };
        info!("txn: {} get({})", txn_id, key);
        res
    }

    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutReply>, tonic::Status> {
        let PutRequest { key, value, txn_id } = request.into_inner();
        let mut txns = self.txns.lock().await;
        let txn = txns.get_mut(&txn_id).unwrap();
        let res = match txn.put(key.clone(), value.clone()).await {
            Ok(()) => Ok(Response::new(PutReply { error: None })),
            Err(err) => match err {
                Error::UndeterminedError(_) | Error::Grpc(_) => Ok(Response::new(PutReply {
                    error: Some(ClientServerError {
                        undetermined: format!("tikv transaction put() failed: {:?}", err),
                        not_found: "".to_owned(),
                        aborted: "".to_owned(),
                    }),
                })),
                _ => Ok(Response::new(PutReply {
                    error: Some(ClientServerError {
                        aborted: format!("tikv transaction put() aborted: {:?}", err),
                        not_found: "".to_owned(),
                        undetermined: "".to_owned(),
                    }),
                })),
            },
        };
        info!("txn: {} put({}, {})", txn_id, key, value);
        res
    }

    async fn commit(
        &self,
        request: tonic::Request<CommitRequest>,
    ) -> Result<tonic::Response<CommitReply>, tonic::Status> {
        let CommitRequest { txn_id } = request.into_inner();
        let mut txns = self.txns.lock().await;
        let txn = txns.get_mut(&txn_id).unwrap();
        let res = match txn.commit().await {
            Ok(_) => Ok(Response::new(CommitReply { error: None })),
            Err(err) => match err {
                Error::UndeterminedError(_) | Error::Grpc(_) => Ok(Response::new(CommitReply {
                    error: Some(ClientServerError {
                        undetermined: format!("tikv transaction commit() failed: {:?}", err),
                        not_found: "".to_owned(),
                        aborted: "".to_owned(),
                    }),
                })),
                _ => Ok(Response::new(CommitReply {
                    error: Some(ClientServerError {
                        aborted: format!("tikv transaction commit() aborted: {:?}", err),
                        not_found: "".to_owned(),
                        undetermined: "".to_owned(),
                    }),
                })),
            },
        };
        info!("txn: {} commit()", txn_id);
        res
    }

    async fn rollback(
        &self,
        request: tonic::Request<RollbackRequest>,
    ) -> Result<tonic::Response<RollbackReply>, tonic::Status> {
        let RollbackRequest { txn_id } = request.into_inner();
        let mut txns = self.txns.lock().await;
        let txn = txns.get_mut(&txn_id).unwrap();
        let res = match txn.rollback().await {
            Ok(_) => Ok(Response::new(RollbackReply { error: None })),
            Err(err) => match err {
                Error::UndeterminedError(_) | Error::Grpc(_) => Ok(Response::new(RollbackReply {
                    error: Some(ClientServerError {
                        undetermined: format!("tikv transaction rollback() failed: {:?}", err),
                        not_found: "".to_owned(),
                        aborted: "".to_owned(),
                    }),
                })),
                _ => Ok(Response::new(RollbackReply {
                    error: Some(ClientServerError {
                        aborted: format!("tikv transaction rollback() aborted: {:?}", err),
                        not_found: "".to_owned(),
                        undetermined: "".to_owned(),
                    }),
                })),
            },
        };
        info!("txn: {} rollback()", txn_id);
        res
    }
}
