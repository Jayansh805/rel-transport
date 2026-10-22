# Reliable UDP Protocol

This project implements the reliable transport layer for a networking
assignment. The code uses UDP packets underneath and provides reliable,
ordered delivery between the local input/output streams and a remote peer.

## Project overview and functionality

### What the project does

This project implements a reliable, ordered byte stream on top of UDP. UDP
only provides datagram delivery and does not guarantee that packets will
arrive, arrive once, or arrive in the order they were sent. The code in
`reliable.c` adds those transport-layer guarantees while `rlib.c` connects
the protocol to local input/output streams and manages the event loop.

Data flows through the project in two directions:

```text
local input
    -> rlib.c calls rel_read()
    -> reliable.c creates and checksums a data packet
    -> rlib.c sends it with conn_sendpkt()
    -> UDP network
    -> rlib.c calls rel_recvpkt()
    -> reliable.c validates and buffers the packet
    -> rlib.c receives data through conn_output()
    -> local output
```

Acknowledgements travel in the opposite direction. They tell the sender which
sequence number the receiver is waiting for, allowing acknowledged packets to
be removed and allowing new input to be read into newly available window
slots.

### Architecture

The implementation has three cooperating layers:

1. **Application and stream layer (`rlib.c`)** - owns file descriptors,
   sockets, non-blocking I/O, output queues, and the `poll()`-based event loop.
2. **Reliable transport layer (`reliable.c`)** - owns sequence numbers,
   acknowledgements, checksums, send and receive buffers, retransmission
   timers, flow control, and connection shutdown state.
3. **UDP layer (`rlib.c` and the operating system)** - carries encoded data
   and acknowledgement packets between peers.

`rlib.c` does not interpret the reliability rules. Instead, it invokes
callbacks in `reliable.c` when application input is available, when a packet
arrives, when output space becomes available, and when a timer expires. This
keeps socket/event-loop responsibilities separate from protocol state.

### Per-connection state

Each connection has one opaque `rel_t` state object. Its important fields are:

- `window` and `timeout` - configured flow-control and retransmission values.
- `unacked_base` - the first outgoing sequence number that has not been
  cumulatively acknowledged.
- `next_seqno` - the sequence number assigned to the next outgoing packet.
- `tx_buffer` - linked list of sent packets retained for acknowledgement
  processing and possible retransmission.
- `expected_seqno` - the next incoming sequence number required for ordered
  delivery.
- `rx_buffer` - linked list of valid packets received ahead of the expected
  sequence number.
- `eof_read` and `eof_received` - indicate whether local input and remote
  input have reached EOF.
- `closing` and `close_ts` - implement the short closing grace period before
  the connection is destroyed.

The sender starts sequence numbering at 1. The receiver also initially expects
sequence number 1. A cumulative acknowledgement of `4` means that packets
1 through 3 have been received in order and packet 4 is the next packet
needed.

### Function-by-function operation

#### `get_time_ms`

Returns a monotonic timestamp in milliseconds. It is used instead of wall
clock time so changes to the system clock do not affect retransmission
decisions.

#### `send_ack`

Builds an 8-byte acknowledgement-only packet. The acknowledgement contains
`expected_seqno`, which is the next packet the receiver needs, and includes a
checksum before being sent with `conn_sendpkt`.

#### `check_destroy`

Checks whether both directions have completed and all outgoing data has been
acknowledged. When local input has reached EOF, a remote EOF has been
delivered, and `tx_buffer` is empty, the connection enters its closing state.
Actual destruction is delayed by `rel_timer` to give final network activity
time to complete.

#### `rel_create`

Allocates and initializes a reliable connection. It records the configured
window and timeout, initializes both sequence-number counters to 1, and adds
the connection to the global reliable-connection list. In server mode it can
also request a `conn_t` from `conn_create`.

#### `rel_destroy`

Removes a connection from the global list, schedules its underlying library
connection for destruction, frees every outstanding transmit packet and
receive packet, and finally frees the reliable state object.

#### `rel_read`

Moves application data into the network:

1. It stops immediately if local EOF was already detected.
2. While the send window has capacity, it calls `conn_input` for up to 500
   bytes.
3. It constructs a 12-byte data header containing the next sequence number
   and current acknowledgement number.
4. It computes the checksum, stores a copy in `tx_buffer`, and sends it.
5. A return value of `0` means input is temporarily unavailable; `-1` creates
   a zero-payload EOF packet and stops further local reads.

Packets remain in `tx_buffer` until a later cumulative acknowledgement proves
that the peer received them.

#### `rel_recvpkt`

Processes packets for direct and client connections. It first rejects
datagrams with invalid lengths, invalid protocol sizes, or invalid checksums.
It then processes the packet's cumulative acknowledgement by removing older
entries from `tx_buffer`. If the acknowledgement advanced, `rel_read` is
called to fill the newly available send-window slots.

For a data packet, the function accepts only sequence numbers within the
receive window, ignores duplicates, and stores new packets in `rx_buffer`.
It calls `rel_output` to deliver any now-contiguous packets. If no progress
was possible because the packet was a duplicate or a gap remains, it sends a
repeat acknowledgement so the peer can recover from a lost acknowledgement.

#### `rel_output`

Delivers buffered packets in sequence-number order. It searches for
`expected_seqno` and stops when that packet is not available or when
`conn_bufspace` reports insufficient output capacity. Payload bytes are passed
to `conn_output`; a zero-payload packet is passed as an EOF notification.
After each successful delivery, `expected_seqno` advances. If progress was
made, a cumulative acknowledgement is sent.

This function is also called by the library after queued application output
has drained, so packets paused by flow control can be delivered later.

#### `rel_timer`

Runs periodically from the library event loop. It checks every packet in
`tx_buffer` and retransmits packets whose recorded send time is older than
`timeout`. Each retransmission updates its timestamp and retry count. After
more than five retries, the current implementation gives up and destroys the
connection. The timer also destroys connections that have completed their
closing grace period.

#### `rel_demux`

This callback is used by server mode, where one UDP socket can receive
packets from multiple remote clients. Its intended responsibility is to find
the reliable connection associated with the client's socket address, create a
new connection for a new client, and forward the packet to `rel_recvpkt`.
The function is currently an empty stub in this checkout, so server-mode
demultiplexing is not implemented yet.

### Packet and reliability behavior

Each data packet contains a sequence number, a cumulative acknowledgement
number, a checksum, and up to 500 bytes of payload. The sender retains every
unacknowledged packet. The sliding window limits how many packets may be in
flight, supporting both stop-and-wait operation (`window == 1`) and multiple
outstanding packets.

Incoming packets are checked for valid length and checksum before they are
accepted. Out-of-order packets remain in `rx_buffer` until the missing packet
arrives. Once consecutive packets are available, they are delivered in order
and acknowledged. If a packet or acknowledgement is lost, the retransmission
timer resends the unacknowledged packet.

Flow control is provided by checking `conn_bufspace` before calling
`conn_output`. A zero-payload data packet represents EOF. A connection is
closed only after local input has ended, remote EOF has been delivered, and
all outgoing packets have been acknowledged.

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
