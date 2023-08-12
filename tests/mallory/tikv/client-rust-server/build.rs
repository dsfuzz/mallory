// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/rawkv.proto")?;
    tonic_build::compile_protos("../proto/txnkv.proto")?;
    tonic_build::compile_protos("../proto/error.proto")?;
    Ok(())
}
