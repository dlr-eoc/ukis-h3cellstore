use std::fs::rename;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/clickhouse_grpc.proto");
    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir("src")
        .compile(&["proto/clickhouse_grpc.proto"], &["proto"])?;

    let tonic_output_path = Path::new("src/clickhouse.grpc.rs");
    if tonic_output_path.exists() {
        rename(tonic_output_path, Path::new("src/api.rs"))?;
    }
    Ok(())
}
