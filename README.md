# Experimental Kafka trigger for Spin

## Install from release

```
spin plugins install --url https://github.com/zedgell/spin-trigger-kafka/releases/download/canary/trigger-kafka.json
```

## Build from source

```
cargo build --release
spin pluginify
spin plugins install --file trigger-kakfa.json --yes
```

## Test

```
cd guest
spin build --up
```

## Configuration

Documentation is TBD but see `guest/spin.toml` for an example of the configuration knobs.