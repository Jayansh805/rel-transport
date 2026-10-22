# Reliable UDP Protocol

This project implements the reliable transport layer for a networking
assignment. The code uses UDP packets underneath and provides reliable,
ordered delivery between the local input/output streams and a remote peer.

## Project layout

- `reliable.c` - reliable protocol implementation
- `rlib.c` - supplied networking and event-loop library
- `rlib.h` - packet format, library API, and assignment specification
- `Makefile` - build instructions for the submitted implementation
- `Testing/` - reference implementation and its build files

## Requirements

- GCC
- GNU Make
- A POSIX-compatible environment with sockets and `clock_gettime`

The supplied Makefile targets Unix-like systems. On Windows, build and run
the project through WSL or another POSIX-compatible environment.

## Build

From the project root:

```sh
make
```

This produces the `reliable` executable. To remove compiled output:

```sh
make clean
```

If the local Makefile does not provide a `clean` target, remove generated
`.o` files and the executable manually.

## Usage

The executable takes a local endpoint and a remote endpoint. Endpoints use
the form `port` or `host:port`, depending on the selected mode:

```text
./reliable [options] local-endpoint remote-endpoint
```

### Direct connection

Without `-c` or `-s`, the program connects standard input and standard output
to a single UDP peer:

```sh
./reliable local-port remote-host:remote-port
```

### Client mode

Client mode accepts TCP connections on the local endpoint and forwards their
data over UDP to the server:

```sh
./reliable -c local-port server-host:server-port
```

### Server mode

Server mode receives UDP traffic on the local endpoint and forwards each
demultiplexed connection to the destination endpoint:

```sh
./reliable -s local-port destination-host:destination-port
```

### Useful options

- `-w WINDOW` - number of unacknowledged packets allowed in flight
- `-t TIMEOUT` - retransmission timeout in milliseconds
- `-d` - print packet debugging information
- `-l` - write input and output traffic logs
- `-u` - use Unix-domain endpoints where supported by the selected mode

For example:

```sh
./reliable -w 4 -t 1000 -d 5000 remote-host:6000
```

## Protocol behavior

The protocol uses cumulative acknowledgements and packet sequence numbers.
Data packets contain a 12-byte header followed by up to 500 bytes of payload;
an 8-byte packet is an acknowledgement-only packet. A data packet with no
payload communicates end-of-file.

The implementation is responsible for:

- maintaining per-connection send and receive state;
- enforcing the configured sliding window;
- validating packet lengths and checksums;
- buffering out-of-order packets;
- delivering packets in order;
- acknowledging received data;
- retransmitting unacknowledged packets after the timeout; and
- releasing connections after both directions reach EOF and sent data is
  acknowledged.

## Testing

The `Testing/` directory contains a separate reference build:

```sh
cd Testing
make
```

Generated object files, executables, and debug logs are intentionally ignored
by Git. Source files and Makefiles remain tracked.
