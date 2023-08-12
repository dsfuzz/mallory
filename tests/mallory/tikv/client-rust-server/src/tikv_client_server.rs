// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod raw {
    tonic::include_proto!("tikv.raw"); // The string specified here must match the proto package name
}

pub mod txn {
    tonic::include_proto!("tikv.txn"); // The string specified here must match the proto package name
}
pub mod error {
    tonic::include_proto!("tikv.error"); // The string specified here must match the proto package name
}
