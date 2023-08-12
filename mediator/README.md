# Mediator

Build with static LIBC:

```
sudo apt-get install musl-tools
rustup target add x86_64-unknown-linux-musl

# Build command
cargo build --target=x86_64-unknown-linux-musl 
# OR
RUSTFLAGS="-C target-cpu=native" cargo build --release --target=x86_64-unknown-linux-musl

If you want to build with self-checking:

`RUSTFLAGS="-C target-cpu=native" cargo build --release --target=x86_64-unknown-linux-musl --features selfcheck`

If you want to build with log-saving:

`RUSTFLAGS="-C target-cpu=native" cargo build --release --target=x86_64-unknown-linux-musl --features logsaving`

```

### Debugging

Rust debugging seems very bad, so this has been mostly useless.

#### GDB

```
rust-gdb --args /host/mediator/target/x86_64-unknown-linux-musl/release/rmed qlearning event_history
```

#### LLDB

https://lldb.llvm.org/use/tutorial.html


```
rust-lldb /host/mediator/target/x86_64-unknown-linux-musl/release/rmed -- qlearning event_history
breakpoint set -f summary.rs -l 457
run
```