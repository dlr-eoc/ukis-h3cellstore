

# print the descriptions of all crates - to manually paste it in the readme
print-desc:
    #!/usr/bin/env python
    import os, os.path, toml
    for item in os.listdir("crates"):
        item = os.path.join("crates", item)
        if os.path.isdir(item):
            manifest = os.path.join(item, "Cargo.toml")
            if os.path.exists(manifest):
                t = toml.load(manifest)
                pkg = t["package"]
                print(f"- **{pkg['name']}**: {pkg.get('description') or '-'}")


clickhouse:
    podman run --rm -it \
        -u 101 \
        -v $PWD/dev/clickhouse-server/config.xml:/etc/clickhouse-server/config.xml \
        -p 9100:9100 \
        -p 8123:8123 \
        clickhouse/clickhouse-server:22.8
