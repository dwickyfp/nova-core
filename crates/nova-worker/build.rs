fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["../../proto/nova_rpc.proto"], &["../../proto"])?;
    println!("cargo:rerun-if-changed=../../proto/nova_rpc.proto");
    Ok(())
}
