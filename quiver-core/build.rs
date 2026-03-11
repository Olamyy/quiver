fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = &[
        "../proto/v1/types.proto",
        "../proto/v1/serving.proto",
        "../proto/v1/metadata.proto",
        "../proto/v1/Flight.proto",
        "../proto/v1/observability.proto",
    ];

    let proto_includes = &["../proto/v1"];

    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("quiver_descriptor.bin"))
        .compile_protos(proto_files, proto_includes)?;

    for file in proto_files {
        println!("cargo:rerun-if-changed={}", file);
    }

    Ok(())
}
