# libMQTTxt MQTT Client Library for OXT

A pure OXT implementation of MQTT 3.1.1 protocol client with full QoS support, TLS encryption, and automatic reconnection.

## Features

- **Full MQTT 3.1.1 Compliance** - All packet types, QoS levels, and features
- **TLS/SSL Support** - Secure connections to encrypted brokers
- **Quality of Service** - QoS 0, 1, and 2 with proper handshaking
- **Wildcards** - Subscribe using `+` and `#` wildcards
- **Retained Messages** - Publish and receive retained messages
- **Last Will Testament** - Configure LWT for abnormal disconnections
- **Auto-Reconnect** - Automatic reconnection with exponential backoff
- **Persistent Sessions** - Resume sessions across reconnections
- **Keep-Alive** - Automatic PINGREQ/PINGRESP handling
- **Multiple Connections** - Connect to multiple brokers simultaneously
- **Statistics** - Track messages, bytes, and connection metrics
- **Binary Safe** - Full support for binary message payloads

## Requirements

- OXT or LCC 9.6.3
- Network access to MQTT broker

## Try it first

`examples/mqtt-dashboard.livecodescript` is the demo: connect to a broker,
subscribe, publish, and watch the traffic. **It is one file.** Paste it into a
stack script and reopen the stack — the library is embedded in it, so there is
nothing else to load and no wiring step.

It prints its own boot record, too: on open it runs a non-destructive
self-check (nothing is connected, bound, or published) and logs `PASS`/`FAIL`
per assertion with a count line. That block is what an engine pass quotes,
instead of "the window built and it looked right".

## Installation

There are two ways to take the library, and they differ in exactly one line.

**As a library stack** — the right dependency for a real project:

```OXT
on preOpenStack
   start using stack "libMQTTxt.oxtstack"
end preOpenStack
```

**Embedded in your own stack script** — copy `libMQTTxt.oxtstack`'s contents
above your own code (this is what the demo does, and what
`tools/sync-demo-embeds.py` automates):

```OXT
on preOpenStack
   -- The engine sends `libraryStack` only to a stack in stacksInUse, so an
   -- embedded copy never receives it and its globals stay uninitialised.
   -- This is the one line an embedding stack owes the library.
   mqttInitialize
end preOpenStack
```

Embedding also means you inherit the library's `on socketClosed` /
`on socketError` / `on socketTimeout`. If your stack runs sockets of its own
and needs to define those three itself, see
[Co-existing with other socket libraries](#co-existing-with-other-socket-libraries).

## Quick Start

```OXT
-- Configure callbacks
mqttSetCallbackTarget the long id of this card
mqttSetMessageCallback "onMessage"

-- Connect to broker
put mqttConnect("broker.hivemq.com", 1883, "myClient", "", "", \
                60, false, true, "", "", 0, false, true, false, "") into tResult

-- Wait for connection
wait until mqttIsConnected("broker.hivemq.com", 1883) with messages

-- Subscribe to topic
mqttSubscribe "broker.hivemq.com", 1883, "test/topic", 0

-- Publish message
mqttPublish "broker.hivemq.com", 1883, "test/topic", "Hello MQTT", 0, false

-- Handle incoming messages
on onMessage pTopic, pMessage
   put pTopic & ":" && pMessage
end onMessage
```

## Documentation

- **[Complete API Reference](libMQTTxt_Reference.md)** - Full function documentation with examples
- **[The demo](examples/mqtt-dashboard.livecodescript)** - one paste-and-run file; the shortest path to seeing the library work
- **Built-in Self-Test** - Call `mqttSelfTest()` to verify the library's internal encoders and helpers
- **[docs/ENGINE-NOTES.md](docs/ENGINE-NOTES.md)** - what OXT and real brokers actually do, and what each fact cost to learn
- **[tools/VENDORED.md](tools/VENDORED.md)** - what this repo carries from [xtalk-suite](https://github.com/SethMorrowSoftware/xtalk-suite), and how to re-sync it

## Usage Examples

### Basic Connection

```OXT
-- Simple connection
put mqttConnectSimple("broker.example.com", 1883, "client1", "user", "pass") into tResult

if tResult is "OK" then
   put "Connected"
end if
```

### TLS Connection

```OXT
-- Connect with TLS. pVerifyTLS defaults to TRUE; pass it explicitly here so the
-- intent is visible at the call site rather than inherited from a default.
put mqttConnect("broker.example.com", 8883, "secureClient", \
                "username", "password", 60, true, true, \
                "", "", 0, false, true, true, "") into tResult

-- Pin a CA bundle when the broker uses a private or self-signed authority
put mqttConnect("broker.example.com", 8883, "secureClient", \
                "username", "password", 60, true, true, \
                "", "", 0, false, true, true, \
                specialFolderPath("resources") & "/ca-bundle.pem") into tResult
```

> **Changed in 2.12.0.** `pVerifyTLS` used to default to `false`, so leaving it
> empty produced an encrypted channel that authenticated nobody - proof against
> a passive listener, worthless against an active one. It now defaults to
> `true`. Passing `false` still works and is still supported; it now logs a
> warning, because an unverified TLS socket should be a decision somebody made
> rather than one they inherited.

### Subscribe with Wildcards

```OXT
-- Subscribe to all sensors
mqttSubscribe "broker.example.com", 1883, "sensor/#", 0

-- Subscribe to temperature from any room
mqttSubscribe "broker.example.com", 1883, "room/+/temperature", 0
```

### QoS Levels

```OXT
-- QoS 0 - Fire and forget
mqttPublish "broker.example.com", 1883, "data", "value", 0, false

-- QoS 1 - At least once delivery
mqttPublish "broker.example.com", 1883, "data", "value", 1, false

-- QoS 2 - Exactly once delivery
mqttPublish "broker.example.com", 1883, "critical", "command", 2, false
```

### Retained Messages

```OXT
-- Publish retained message
mqttPublish "broker.example.com", 1883, "device/status", "online", 0, true

-- Clear retained message
mqttPublish "broker.example.com", 1883, "device/status", "", 0, true
```

### Last Will Testament

```OXT
-- Configure LWT - published if client disconnects unexpectedly
put mqttConnect("broker.example.com", 1883, "device1", "", "", 60, \
                false, true, "device/status", "offline", 1, true, \
                true, false, "") into tResult
```

### Auto-Reconnect

```OXT
-- Enable automatic reconnection
mqttSetReconnectCallback "onReconnect"

put mqttConnect("broker.example.com", 1883, "resilient", "", "", 60, \
                false, true, "", "", 0, false, true, true, "") into tResult

on onReconnect pEvent, pHost, pPort, pAttempts
   if pEvent is "success" then
      put "Reconnected after" && pAttempts && "attempts"
   end if
end onReconnect
```

### Connection Statistics

```OXT
put mqttGetStatistics("broker.example.com", 1883) into tStats

put "Messages sent:" && tStats["messagesSent"]
put "Messages received:" && tStats["messagesReceived"]
put "Bytes sent:" && tStats["bytesSent"]
put "Reconnections:" && tStats["reconnections"]
```

## Configuration

### Callbacks

```OXT
-- Set callback target. Recommended, no longer required: since 2.12.0 the
-- setters below capture the calling stack if no target has been set, rather
-- than discarding every callback in silence. Set it explicitly when the
-- handlers live somewhere other than the stack doing the registering.
mqttSetCallbackTarget the long id of this card

-- Set message callback
mqttSetMessageCallback "handleMessage"

-- Set log callback
mqttSetLogCallback "handleLog"

-- Set state change callback
mqttSetStateChangeCallback "handleStateChange"

-- Set reconnect callback
mqttSetReconnectCallback "handleReconnect"
```

### Options

```OXT
-- Enable debug logging
mqttSetDebugMode true

-- Disable all logging
mqttSetQuietMode false

-- Set keep-alive threshold (75% default)
mqttSetKeepAliveThreshold 0.75

-- Set maximum buffer size (5MB default)
mqttSetMaxBufferSize 5242880

-- Largest single socket write handed to the kernel (16KB default). Lower it
-- if large publishes time out on a cold connection; see docs/ENGINE-NOTES.md 1.1
mqttSetWriteChunkSize 16384

-- Enable persistent storage for QoS 1/2
mqttSetPersistentStore true, specialFolderPath("documents") & "/mqtt"
```

## API Overview

### Connection Management

| Function | Description |
|----------|-------------|
| `mqttConnect()` | Connect to MQTT broker with full options |
| `mqttConnectSimple()` | Connect with common defaults |
| `mqttIsConnected()` | Check connection status |
| `mqttReconnect()` | Manually reconnect |
| `mqttDisconnect()` | Disconnect from broker |
| `mqttCleanupAll()` | Disconnect all connections |

### Publishing & Subscribing

| Function | Description |
|----------|-------------|
| `mqttPublish()` | Publish message to topic |
| `mqttSubscribe()` | Subscribe to topic filter |
| `mqttUnsubscribe()` | Unsubscribe from topic |
| `mqttGetSubscriptions()` | List active subscriptions |

### Utilities

| Function | Description |
|----------|-------------|
| `mqttGetConnectionInfo()` | Get connection details |
| `mqttGetStatistics()` | Get message/byte counters |
| `mqttResetStatistics()` | Reset statistics |
| `mqttGetSessionPresent()` | Check session persistence |
| `mqttGetConnections()` | List all connections |
| `mqttTestLibrary()` | Verify library loaded |
| `mqttSelfTest()` | Run internal tests |
| `mqttBenchmark()` | Performance benchmark |

## QoS Levels

| QoS | Description | Use Case |
|-----|-------------|----------|
| 0 | At most once | Fire and forget, sensor data |
| 1 | At least once | Important data, duplicates OK |
| 2 | Exactly once | Critical commands, no duplicates |

## Wildcards

| Wildcard | Matches | Example |
|----------|---------|---------|
| `+` | Single level | `sensor/+/temp` matches `sensor/1/temp` |
| `#` | Multiple levels | `sensor/#` matches `sensor/1/temp/celsius` |

## Error Handling

All functions return `"OK"` on success or `"ERROR: description"` on failure.

```OXT
put mqttPublish("broker.example.com", 1883, "topic", "msg", 0, false) into tResult

if tResult is not "OK" then
   answer error tResult
   exit to top
end if
```

## Testing

### Static gates (run in CI, no engine required)

OXT has no headless way to compile or run a `.oxtstack` or a
`.livecodescript`, so the checks that CI *can* run stand in for the compiler it
cannot. One command runs them all, in the order that makes each one's result
mean something:

```sh
tools/run-gates.sh
```

Nothing but Python 3 is needed. Individually:

| Gate | What it holds |
|------|---------------|
| `test-check-libmqttxt.py` | that the library gate still fires on each defect |
| `test-demo-gates.py` | that the five demo gates still fire on each defect |
| `check-libmqttxt.py` | ASCII purity, `does not contain` and relatives, undeclared `catch` variables, calls to helpers that do not exist, character-counting in binary framing, a missing or swallowed engine socket message, an unrouted timer handler, a per-byte socket read, and any control reference in a headless library |
| `test-mqtt-vectors.py` | the framing, against MQTT 3.1.1 byte sequences |
| `sync-demo-embeds.py --check` | the demo's embedded library copy is current, and collides with nothing |
| `check-carried-blocks.py` | the UI kit and boot self-check are byte-identical to their masters, adopted deliberately, and actually run |
| `check-demo-control-lists.py` | the self-check's control list is re-derived from the demo's own source |
| `check-demo-layout.py` | the window fits 720p and every control fits the window, its panel, and no neighbour |
| `check-timer-stack-pin.py` | every `send … in` handler pins the defaultStack before touching a control |

**Every rule is a defect that actually shipped**, here or in the suite this
tooling comes from, written down so it cannot ship twice.

The two `test-*` entries matter as much as the gates do, which is why they run
first: **a gate that has gone blind reports OK, and OK is exactly what a blind
gate and a clean tree look like from the outside.** They reintroduce each defect
into a real copy of the tree and fail if the gate stays quiet, so the OKs
underneath are worth reading.

Two of those gates exist because their failures are *invisible without an
engine*, not merely tedious to check: a control pushed past the bottom edge is
simply not there, and an unpinned delayed write lands on the wrong stack or
nowhere at all. Neither says anything at runtime.

### On-engine testing

Open `examples/mqtt-dashboard.livecodescript`. It runs a **boot self-check** on
open — 12 assertions, none of which connects, binds, or publishes — and prints
`PASS`/`FAIL` per line with a count trailer into its own log. Select that block
and paste it into the pass record: it is the citable half of an engine session,
and it exists because the alternative is a human judgement ("the window built,
it looked right") that no honesty label can quote.

The check is deliberately non-destructive. Three of its assertions prove a
*refusal* — an invalid port, an unbracketed IPv6 host, publishing while
disconnected — because a check that took a socket would be indistinguishable
from a broker that refused it, and the operator is about to press Connect for
real.

`mqttSelfTest()` runs the library's pure-compute checks inside the engine and
returns a report; a failing run lists which assertions failed, not just how
many. The demo's Self-test button runs it and logs the result.

Anything involving a live broker — QoS flows, TLS, reconnect, wildcards, large
payloads — needs a real engine and a real broker.

### What a real run has established

**First engine pass: 2026-09-05**, on OXT against a local mosquitto and
`broker.hivemq.com`. Recorded in [docs/ENGINE-NOTES.md](docs/ENGINE-NOTES.md).

Now **observed** rather than argued: the library and demo compile and load; the
demo builds its window; CONNECT is accepted by two independent brokers; the
streaming read works; QoS 0/1/2 round trips complete with their acknowledgment
legs drained and QoS 2 delivered exactly once; **a 13-byte, 7-character UTF-8
payload round-trips byte for byte** — the exact case a character-counted
Remaining Length gets wrong; and all 256 byte values survive intact.

That same run found two things the gates could not:

- **A failed socket write does not throw** — LiveCode reports it in `the
  result`, so all twelve of this library's writes reported success on writes
  that may not have happened. A large PUBLISH left the broker holding a partial
  packet and the connection went silent inbound with no error on either side.
  Fixed in 2.12.1; a gate now refuses a bare `write ... to socket`.
- **`secure socket ... with verification` reported success on a plaintext
  port.** Treat TLS status as unverified regardless of what the log says, and
  confirm a TLS connection reached CONNACK before trusting it. Not yet closed:
  the sixth run showed TLS working end to end against a valid certificate on
  port 8883, which proves the channel and still says nothing about what
  "verified" would do with a bad one.

A second run the same day, against `broker.hivemq.com` with the write check
in place, added **keep-alive end to end** (100s idle, PINGRESP received and
processed), **large payloads to 200 KB**, **retained replay**, **zero-length
payloads** and **unsubscribe** to the observed list. It also found the
conformance test asserting the QoS 2 acknowledgment leg one line too early —
the library was right.

Runs three and four, back on the mosquitto that first failed, confirmed the
diagnosis and located the cause: a synchronous write that exceeds the kernel's
send buffer silently drops its tail. 2.12.3 chunks every write.

The fifth run, on a fresh engine, found the embedded library uninitialised (the
earlier runs had inherited populated globals from a standalone-library session)
and 2.12.4 made the library initialise itself before it touches the network.

The sixth run, 2026-09-06, was the first over **TLS** (verified, against
`broker.hivemq.com:8883`) and the first with **auto-reconnect** ticked. The
conformance run was green at every stage, 14 passed and 0 failed, and the
ladder round-tripped **1 MB** intact. Auto-reconnect's back-off was observed
(1, 2, 4, 8, 16 s, then 30 s with jitter, unbounded) against a port that
refused every attempt, and a manual disconnect stopped it cleanly. It also
found the reconnect log line reporting a clamped attempt number, fixed in
2.12.5.

Runs seven and eight refuted two diagnoses in a row, and **the ninth found the
cause by controlled comparison.** In one run, the same 1 MB publish in the clear
stalled against a local mosquitto over a LAN *and* against `broker.hivemq.com`
over the internet — while the same engine had carried that same megabyte to that
same broker over TLS without a pause. Two brokers, two networks, one variable:
plaintext.

The reading that fits every measurement: the engine's plaintext write hands the
kernel one non-blocking send and does not retry a socket that is momentarily
full. Chunking never addressed that, because chunks written back to back cost no
elapsed time — the send buffer fills faster than the wire drains it.

**The tenth run proved the cure and measured it.** A pacing ladder published a
megabyte at 0, 5, 20 and 50 ms per chunk: the first three stalled at exactly
65536 bytes and 50 ms carried it. Keeping that pause, **the whole conformance run
then went green against the LAN broker — 16 passed, 0 failed** — with the size
ladder to 1 MB, retained replay, unsubscribe and 100 s of keep-alive included.
That is the first fully green run of the project. Pacing is now the default.

**What it costs:** a multi-chunk write is capped at chunk ÷ pause = 320 KB/s,
and the run's timings show 94–96% of a large write's elapsed time is the pause
rather than the network. Payloads that fit one chunk are unaffected. Tune it for
your own path with the conformance button, or wait for asynchronous writes,
which would pace themselves.

**An eleventh session then ran the last untested stage.** Two simultaneous
connections to one broker — reached by two spellings of its address, since the
library keys a connection by `host:port` — were held idle for 100 s at *different*
keep-alive intervals, 30 s against 60 s, and both survived with their own
PINGRESPs. That is the 2.12.0 timer-token work proved on an engine: before it, a
single delayed message served every connection and whichever timer fired first
pinged both. The best run of the project is **17 passed, 0 failed, 1 skipped.**

**Still unproven:** a reconnect that *succeeds* and resubscribes; the persistent
store; and certificate verification rejecting a bad certificate. The performance
figures below are from 2.11.x and have not been re-measured.

## Performance

Measured on 2.11.x, before the read-path change; **not re-measured since**:

- **Throughput**: 200-400 messages/second
- **Latency**: < 5ms per message (QoS 0)
- **Large Messages**: Up to 50KB+ payloads
- **Concurrent**: 5+ simultaneous operations
- **Reliability**: 90%+ delivery in burst tests

2.12.0 replaced a per-byte socket read (one engine message dispatch per inbound
byte) with a streaming read, which should move the large-payload and throughput
numbers substantially. By how much is unmeasured, and stays unclaimed until
somebody runs it.

## Co-existing with other socket libraries

`socketClosed`, `socketError` and `socketTimeout` are the **engine's** message
names, so every socket library in the message path declares the same three, and
no single script may define one of them twice. This library therefore keeps its
logic in ordinary functions - `mqttSocketClosed()`, `mqttSocketError()`,
`mqttSocketTimeout()` - each answering one question: *was that socket mine, and
did I handle it?* The `on` handlers are pure dispatch, and pass anything that is
not ours rather than swallowing it.

If you are pasting this library into a stack that already runs its own sockets,
drop the three `on` wrappers and call the functions from your own handlers:

```OXT
on socketClosed pSocketID
   if mqttSocketClosed(pSocketID) then
      exit socketClosed
   end if
   -- ...your own socket handling...
end socketClosed
```

No logic is copied, so nothing can go stale. Swallowing another library's
socket message is a silent hang for it, which is why the "not mine" branch
always passes.

## Tested Brokers

- HiveMQ Cloud
- Mosquitto
- AWS IoT Core
- Azure IoT Hub
- Eclipse Mosquitto

## Protocol Compliance

MQTT 3.1.1 Specification Implementation:

- ✅ CONNECT / CONNACK
- ✅ PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP
- ✅ SUBSCRIBE / SUBACK
- ✅ UNSUBSCRIBE / UNSUBACK
- ✅ PINGREQ / PINGRESP
- ✅ DISCONNECT
- ✅ QoS 0, 1, 2 flows
- ✅ Session persistence
- ✅ Retained messages
- ✅ Last Will Testament
- ✅ Keep-alive mechanism
- ✅ Wildcard subscriptions
- ✅ TLS encryption

## Version History

### 2.13.0 (Current)

**Writes are asynchronous.** The fix that runs 7 through 10 pointed at, built.

- **Every packet goes through a per-connection outbound queue**, written one
  chunk at a time, each chunk started only when the engine reports the previous
  one done. That is self-pacing: the queue drains at exactly the rate the socket
  accepts, with no delay to guess at. It replaces the fixed 50 ms pause that
  worked but capped large writes at 320 KB/s while the real write work for a
  megabyte was ~139 ms.
- **Every packet, not just the large ones**, because that is what makes it safe.
  A PINGREQ written directly while a megabyte of PUBLISH sat half-queued would
  land *inside* that publish and desynchronise the broker's framing for good.
  One queue per socket, strictly FIFO, one chunk in flight — there is no direct
  path, and a static gate enforces that only the pump may start a write.
- **`mqttPublish` returning `OK` now means queued, not written.** A failure
  after that point arrives through the state-change callback with
  `disconnected` and a reason. For QoS 1 and 2 the acknowledgment was always the
  real proof of delivery. `mqttGetQueuedBytes` reports what is still owed.
- **Backpressure:** a packet that would take the queue past the buffer ceiling
  is refused, rather than growing it until the engine runs out of memory.
- **`DISCONNECT` is still written synchronously**, after dropping the queue. A
  queued one would be discarded by the `close socket` on the next line and every
  clean disconnect would fire the Last Will.
- **`mqttSetAsyncWrites false`** falls back to the 2.12.8 paced synchronous path
  in one line — the way back if this misbehaves on your engine.
- **New gate:** `tools/test-write-queue.py` transcribes the queue's state
  machine and drives it through the sequences that would break ordering —
  interleaved enqueues, an enqueue arriving during a write, a chunk-size change
  mid-flight, a socket closing mid-packet. That last-but-one case caught a real
  defect in the first draft before it ever ran.

**Run on an engine 2026-09-06**, on Windows 11 against `broker.hivemq.com` —
the queue's first outing and the project's first runs on anything but Linux.
In the clear: 15 passed, 0 failed. Over verified TLS: **16 passed, 0 failed**,
with the boot self-check green at 14 of 14. Nineteen packets interleaved with
PINGREQs and subscription changes across a ladder to 1 MB, every one
acknowledged in sequence.

**A megabyte round-trips in 522 ms**, against the 3150 ms of pure pausing that
2.12.8 imposed arithmetically, and the write call is 10–13 ms at every size.
Those are measured: the first of the two runs caught the conformance button
reporting its own 250 ms poll interval as throughput, so arrival times are now
stamped in the message callback and the stages that cannot measure say
"within N ms, an upper bound" instead of printing a rate.

### 2.12.8

From the tenth engine run — **the first fully green conformance run: 16 passed,
0 failed.**

- **Large writes are paced by default.** The pacing ladder measured it: 0, 5 and
  20 ms per chunk all stalled at exactly 65536 bytes against a broker over the
  internet, and 50 ms carried a megabyte. The stall point is the path's send
  buffer; the pause decides whether the write ever drives into it. So
  `gMQTTWriteYieldMs` now defaults to 50, and a large publish works out of the
  box instead of resetting the connection.
- **Ordinary traffic pays nothing.** A packet that fits one chunk never pauses —
  every control packet and almost every publish. This only governs payloads
  larger than the chunk size.
- **The cost, stated honestly:** a multi-chunk write is capped at
  chunk ÷ pause = 320 KB/s. The run's own timings show 94–96% of the elapsed
  time on large payloads is the pause; the real write work for a megabyte was
  139 ms. On a fast LAN that is an order of magnitude left on the table. Lower
  the pause (or raise the chunk size) once you have measured your own path — the
  conformance button's first stage does exactly that and reports the fastest
  value that works.
- Asynchronous writes are the identified real fix, since the engine reports each
  chunk as it is actually written and so paces itself at the link's true rate.
  Not built: it needs an outbound queue per connection and changes what
  `mqttPublish` returning `OK` means. See `docs/ENGINE-NOTES.md` 1.1.

### 2.12.7

From the eighth and ninth engine runs. The eighth refuted the 2.12.6 diagnosis;
**the ninth found the cause by controlled comparison.**

- **The stall is in plaintext writing, not in any broker.** In one run the same
  1 MB publish stalled against a local mosquitto over a LAN (after 442368 bytes)
  and against `broker.hivemq.com` over the internet (after 65536) — while the
  same engine had carried that megabyte to that same broker over TLS in 117 ms.
  Two brokers, two networks, one variable. Every broker-side explanation is dead.
- **The echo is not the cause either** (the eighth run): a megabyte to a topic
  with no subscriber stalled the same way.
- **The reading that fits everything:** the engine's plaintext write does one
  non-blocking send and does not retry a momentarily full socket. Chunking never
  addressed that, because chunks written back to back cost no elapsed time. The
  stall points are just how much fitted before the send buffer filled, which is
  why they track the network path and not the payload size.
- **`mqttSetWriteYieldMs`** turns the inter-chunk yield into a settable pause —
  the workaround, and the instrument. The conformance button's first stage is
  now a pacing ladder that republishes a megabyte at 0, 5, 20 and 50 ms per
  chunk on fresh connections and reports the smallest pause that works, keeping
  a working value set so the rest of the run retests it.
- The yield and re-entrancy lock from 2.12.6 cost nothing observable: every
  stage before the large payloads passed unchanged, twice.

### 2.12.6

From the seventh engine run, which **refuted the 2.12.3 diagnosis**. See
`docs/ENGINE-NOTES.md` 1.1.

- **A large write now yields to the event loop between chunks.** Chunking on its
  own did not fix the mosquitto path: with 16 KB chunks, six went out and the
  seventh blocked for the full socket timeout. A chunk that succeeds six times
  and then stalls was never too big for the send buffer — the peer stopped
  reading. The reading that fits: *we* stopped reading first. A synchronous
  write never returns to the event loop, so the echo of our own publish backs
  up in our receive buffer, the broker cannot flush to us, and it stops draining
  what we send. One `wait 0 milliseconds with messages` per chunk gives the
  pending read its turn and breaks the stall. It also stops a megabyte publish
  from freezing the UI and starving the keep-alive timers.
- **The yield is re-entrant, so multi-chunk writes take a lock.** Application
  code can run during a yield; a callback that publishes would write a second
  packet into the middle of the first. A re-entrant write is refused with an
  error rather than interleaved, and the socket is re-checked after every yield.
  Single-chunk writes — every control packet, `DISCONNECT` included — never
  yield and never lock, so the clean-disconnect guarantee is unchanged.
- **A new gate:** `wait ... with messages` is refused anywhere in the library
  except the one handler written to survive being re-entered.
- **The conformance run gets the experiment that settles it:** 1 MB published to
  a topic nothing is subscribed to, before the ladder, judged by its PUBACK. No
  echo, no inbound pressure. If that passes where the ladder fails, the echo is
  the cause; if it fails too, the write path is.

### 2.12.5

From the sixth engine run: the first over TLS, the first with auto-reconnect
ticked, and the first green conformance run at every rung of the ladder.

- **The reconnect log reports the real attempt number.** The back-off clamps the
  exponent at 16 so `2 ^ n` cannot overflow across a long outage; the log line
  reused the clamped variable and read `attempt 17` for forty attempts running.
  The count and the exponent are now separate, and the callback, which always
  read the counter directly, is unchanged.
- **Self-initialisation is logged.** When `mqttConnect` finds the library
  uninitialised it now says so (`Library initialised on first use ...`), so an
  embedder whose `preOpenStack` did not take effect sees it in the log instead
  of deducing it from a missing line. The sixth run could only infer that this
  path had carried its connection; the next one can observe it.
- The conformance ladder's 1 MB PASS line states what it saw and no longer
  claims why; the first 1 MB pass came over TLS to a broker that had carried
  200 KB before chunking existed, which proves the size and not the mechanism.
  The run's cleanup also no longer unsubscribes a filter it has already dropped.
- The demo header tells the truth about six engine runs, and says REOPEN: applying
  a script to an open stack sends neither `preOpenStack` nor `openStack`, which
  the sixth run demonstrated by getting no boot block at all.
- `docs/ENGINE-NOTES.md`: 1.1 (1 MB observed, mechanism still inferred), 1.2
  (TLS end to end observed; "verified" still unproven), new 1.5 (auto-reconnect
  observed, unbounded by design), 2.1 (the backstop inferred to have carried
  the connection), and the sixth run record.

### 2.12.4

From the fifth engine run, the first on a fresh engine session, and the first
time the boot self-check failed for a real reason.

- **The library initialises itself before touching the network.** Every earlier
  run had inherited populated globals from a standalone-library session in the
  same engine, so the embedded initialisation path had never actually been
  exercised. On a fresh engine the demo's `preOpenStack` did not take effect
  and the boot check reported the keep-alive threshold empty. Had a connection
  succeeded, the first inbound byte would have been torn down as a buffer
  overflow: an empty ceiling compares as text, and any number is greater than
  `""`. `mqttConnect` now calls `mqttInitialize` if it has not run, and the
  buffer ceiling is read through a guarded accessor. `mqttInitialize` from
  `preOpenStack` is still the right thing for an embedder to do; it is no
  longer the only thing standing between the host and empty globals.
- **The demo's boot check asks two questions where it asked one**: did
  `preOpenStack` fire, and is the library initialised. The combined line failed
  and it took the log's first line to work out which half.
- The demo also calls `mqttInitialize` from its start handler, and logs a hint
  when port 8883 is used without TLS, which is what the fifth run tried.
- `docs/ENGINE-NOTES.md` gains a Lifecycle section (2.1, 2.2). The engine's
  `socketClosed` reaching the library is now observed, not argued.

### 2.12.3

The write path, settled by the fourth engine run and an independent analysis of
all four. See `docs/ENGINE-NOTES.md` 1.1 for the evidence.

- **Every socket write is now chunked.** The engine's synchronous write makes
  one `send()`: what fits in the kernel's TCP send buffer is accepted in memcpy
  time, and what does not fit is silently dropped when the wait times out. The
  buffer is autotuned, so **no single write size is safe on a cold connection**.
  `__writeSocket` now hands the kernel at most `mqttSetWriteChunkSize` bytes per
  write (default 16 KB), and a failure reports how far it got: `timeout after
  131072 of 204800 bytes`.
- **The conformance ladder runs to 1 MB.** That rung is the experiment: it
  round-trips only if the chunked write is doing its job, and a chunk that still
  blocks would mean the diagnosis is wrong.
- **Asynchronous writes were considered and rejected**, for now: the engine
  discards queued writes on `close socket`, and both disconnect paths write
  DISCONNECT then close, so every clean disconnect would have published the
  Last Will. Recorded in the notes with the fallback conditions.
- **A related hazard is recorded, not fixed** (notes 1.4): with unread inbound
  data pending at `close`, the DISCONNECT may be lost to an RST and the broker
  fires the Will on a clean exit. Inferred, unobserved, and it names the test
  that would settle it.

### 2.12.2

One change, from the third engine run of the day (mosquitto on a LAN):

- **A failed write now resets the connection.** 2.12.1 caught the failure and
  reported `ERROR: timeout` — then left the connection up with a corrupt output
  stream, so every later write was read by the broker as the tail of the broken
  packet and nothing was acknowledged, echoed, or pinged back. Reporting was
  half the fix. `__failWrite` now tears the connection down at the instant any
  write fails and schedules a reconnect if one is enabled; the caller gets
  `ERROR: <reason> (connection reset)` and a `disconnected` state change.
- The write ceiling is **path-dependent**: 64 KB passes and 128 KB fails on
  mosquitto over a LAN; 200 KB passes on hivemq over the internet. The cause is
  not yet established and the conformance ladder now times every write to
  settle it. See `docs/ENGINE-NOTES.md` 1.1.

### 2.12.1

Everything here came from the first run on a real engine (2026-09-05), and
neither item was reachable by any static gate.

- **A failed socket write does not throw.** LiveCode reports it in `the result`,
  so `try / write ... to socket / catch` catches nothing and the caller is told
  a write succeeded that may not have. A 204800-byte PUBLISH left the broker
  holding a fixed header promising bytes that never arrived; it then read
  everything sent afterwards as that packet's tail, so it stopped acknowledging,
  stopped echoing and answered no PINGREQ — while the client saw clean writes
  and a socket the engine still called open. All twelve writes now route through
  `__writeSocket`, which checks both; a failed PUBLISH also drops its pendingAcks
  entry, so a message the caller was told had failed is not retransmitted as a
  DUP on the next reconnect.
- **The conformance run's keep-alive assertion was too weak** and passed on a
  link that was dead inbound. It now requires `lastPingTime` to be 0 — the value
  `__parsePingResp` zeroes — so an unanswered ping fails.
- **A failing run now says its later results are suspect.** After a broken
  receive path, "nothing is delivered after unsubscribing" passed because
  nothing was being delivered at all.
- The large-payload stage is a **size ladder** (4 KB → 200 KB) reporting the
  largest size that round-trips, so a failure names a threshold rather than a
  symptom.
- `docs/ENGINE-NOTES.md` records both engine facts under the suite's evidence
  rule, including `secure socket ... with verification` reporting success on a
  plaintext port.

### 2.12.0

A correctness and robustness pass. Three of these are behaviour changes that
existing code can notice; they are listed first and deliberately.

**Breaking / behavioural**

- **TLS certificate verification now defaults to ON.** `pVerifyTLS` used to
  default to `false`, so "just turn TLS on" produced an encrypted channel that
  authenticated nobody. Callers who genuinely want an unverified socket must now
  pass `false` explicitly. An unverified handshake also logs a warning.
- **The engine's socket messages are declared under their real names.** The
  library defined `on mqttSocketClosed` / `on mqttSocketError`; the engine only
  ever sends `socketClosed` / `socketError` / `socketTimeout`, so **nothing ever
  called them** - a dropped connection fired no state-change callback, cancelled
  no timer, and auto-reconnect was unreachable code. Those three are now
  declared, each as a thin wrapper over an `mqttSocket*` **function** that
  answers "was that socket mine?", passing anything that is not. See
  [Co-existing with other socket libraries](#co-existing-with-other-socket-libraries).
- **A callback target is captured automatically.** Setting a callback without
  first calling `mqttSetCallbackTarget` used to discard every message silently.
  The setters now record the calling stack as the default target.

**Correctness**

- Two `does not contain` expressions were not valid xTalk. The parser errors on
  `does`, and it fails the **whole script**, not one handler.
- Packet framing counted characters, not bytes. Any non-ASCII topic or payload
  produced a Remaining Length that disagreed with the bytes actually sent, and
  desynchronised the broker from that packet on. All framing is byte-exact now.
- Per-connection timers were routed by scanning every connection for a marker.
  With two connections open, whichever timer fired first serviced **both**, so a
  connection was pinged on its neighbour's schedule. Timers now carry a token
  naming the one connection they belong to.
- Reopening a socket scheduled a phantom reconnect from the queued
  `socketClosed` of the socket we had just closed ourselves, doubling attempts
  per retry. Intentional closes are now marked and ignored.
- `mqttDisconnect` did all of its cleanup inside `if mqttIsConnected(...)`, so
  disconnecting an already-dropped connection cancelled no timers, freed no
  socket-index entry and fired no callback.
- The receive parser wrote its buffer back unconditionally, which **recreated**
  a connection a message callback had just disconnected - a record holding
  nothing but a buffer, which the keep-alive sweep then walked forever.
- `__cleanupConnection` never cancelled the keep-alive, so every failed
  connection left a timer waking forever.
- PUBLISH parsing trusted the declared topic length. A truncated or hostile
  packet produced a clamped topic and a payload read from the wrong offset,
  delivered to the application as a real message.
- QoS 2 resends re-sent the original PUBLISH after the broker had already
  PUBRECed it, restarting a half-completed handshake - the one thing QoS 2
  exists to prevent. The PUBREL leg is now tracked and resent instead.
- SUBACK looked up its topic by UTF-8 bytes in a table keyed by text, so a
  refused non-ASCII subscription was never removed and was re-sent on every
  reconnect. A broker-granted lower QoS is now recorded too.
- Packet IDs could be reused while still in flight after wrapping at 65535.
- CONNECT could set the password flag without the user-name flag, which MQTT
  3.1.1 forbids.
- `mqttSelfTest` overwrote its own failure log with the summary header, so a
  failing run reported counts and no detail. One of its seven tests was a
  tautology; it is now a remaining-length round trip across every varint
  boundary, plus a byte-exactness check.
- Persistent-store writes reported success on a full or read-only disk: `create
  folder` and URL writes set `the result` rather than throwing, and nothing read
  it. The store is also probed for writability when it is enabled, and the
  on-disk copy is no longer deleted before the messages are safely re-saved.
- Reconnect back-off computed `2 ^ attempts` with an unbounded exponent.
- Every `catch` variable is declared. On strict OXT an undeclared one throws a
  second error when the catch fires, masking the failure being reported.

**Performance**

- Inbound reads used `read from socket ... for 1`, which costs **one engine
  message dispatch per byte** - a 1 MB payload arrived as a million round trips
  through the message queue. The library now uses the no-quantifier form, which
  streams whatever has arrived.
- An oversized packet is refused as soon as its declared length is known, rather
  than after it has been buffered.
- The keep-alive sweep runs on the shorter of the ping threshold and the ping
  deadline, so a stalled broker is noticed in time rather than up to two
  keep-alive periods late. `keepAlive = 0` no longer spins a zero-delay timer.
- `__scheduleMessage` diffed the pending-message list as one growing string,
  re-scanned per pending message; it uses an array now.

**The demo, and one library change it forced**

- `examples/mqtt-dashboard.livecodescript`: connect, subscribe, publish, watch
  the traffic. One paste-and-run file, on the xTalk suite's UI kit, carrying
  the suite's boot self-check.
- **`mqttInitialize` is new, and embedding is why.** The engine sends
  `libraryStack` only to a stack in stacksInUse, so a copy embedded in an
  application's own stack script never received it: every global stayed empty
  and the first connection ran against an uninitialised buffer limit and
  keep-alive threshold. Initialisation is now a public command an embedding
  stack calls from `preOpenStack`; `libraryStack` calls it too, so the
  library-stack path is unchanged. The `stacksInUse` warning moved to that path
  alone — it asks "did you forget `start using`?", which is nonsense for an
  embedded copy where the answer is always no and the setup is nonetheless
  correct.

**Tooling** — `tools/` carries the static gates described under
[Testing](#testing), and `tools/run-gates.sh` runs them all. Two of them —
layout and timer-pinning — guard failures that are *invisible without an
engine*: a control past the bottom edge is simply not there, and an unpinned
delayed write lands on the wrong stack or nowhere. The UI kit and boot
self-check are vendored from
[xtalk-suite](https://github.com/SethMorrowSoftware/xtalk-suite); see
[tools/VENDORED.md](tools/VENDORED.md) for what that coupling is and, more to
the point, what it is not.

### 2.11.9
- Critical fix: removed leftover debug `answer` dialogs from `mqttSetCallbackTarget`
- Critical fix: incoming messages now dispatch directly to the configured message handler
- TLS certificate verification (`pVerifyTLS`) and CA bundle (`pCACertPath`) are now honored
- Robust timer-ID capture; keep-alive timers no longer accumulate across multiple connections
- Corrected `stacksInUse` self-check and added missing local variable declarations
- Documentation: fixed broken links and installation file names

### 2.11.8
- Fixed empty topic validation in mqttPublish
- Fixed QoS 2 message statistics tracking
- Improved debug logging in message callback invocation
- Enhanced message reception validation

### 2.11.7
- Critical fix: Timer send uses 'me' instead of stack reference
- Fixed direct logging in timer handlers
- Improved global variable declarations

### 2.11.5
- Timer handlers no longer use parameters
- Connection ID stored in connection record
- Marker-based timer approach

## Architecture

The library is implemented as a script-only stack with:

- **Socket Management** - OXT native socket handling
- **Packet Encoding/Decoding** - Binary encoding for MQTT packets
- **State Machine** - Connection and QoS state tracking
- **Timer Management** - Keep-alive and reconnection timers
- **Buffer Management** - Automatic receive buffer handling
- **Callback System** - Event-driven message delivery

## Limitations

- Single-threaded (runs on main OXT thread)
- Maximum message size limited by `mqttSetMaxBufferSize` (default 5MB)
- Socket callbacks require library in `stacksInUse`
- Topic length limits depend on broker (typically 256 characters)

## Common Use Cases

### IoT Sensor Data
```OXT
-- Publish sensor readings with QoS 0
mqttPublish "broker.example.com", 1883, "sensor/temp", tReading, 0, false
```

### Command & Control
```OXT
-- Send critical commands with QoS 2
mqttPublish "broker.example.com", 1883, "device/command", "SHUTDOWN", 2, false
```

### Status Monitoring
```OXT
-- Publish retained status messages
mqttPublish "broker.example.com", 1883, "device/status", "online", 0, true
```

### Data Logging
```OXT
-- Subscribe to all device data
mqttSubscribe "broker.example.com", 1883, "device/#", 1
```

## Troubleshooting

### Connection Fails
- Verify broker hostname and port
- Check network connectivity
- Confirm credentials (username/password)
- Verify TLS settings match broker requirements

### Messages Not Received
- Confirm subscription was successful
- Check message callback is set
- Verify topic matches subscription filter
- Check QoS level compatibility

### Socket Callback Errors
- Ensure library is in `stacksInUse`
- Verify callback target is set correctly
- Check callback handler exists in target

### Performance Issues
- Increase buffer size with `mqttSetMaxBufferSize`
- Use QoS 0 for high-throughput scenarios
- Consider message batching for bulk data

## Contributing

This is a production-ready library with comprehensive test coverage. For issues or enhancements, ensure changes maintain MQTT 3.1.1 compliance and pass the full test suite.

## License

Open source. Free to use in commercial and non-commercial projects.

## Support

- Documentation: [libMQTTxt_Reference.md](libMQTTxt_Reference.md)
- Self-Test: call `mqttSelfTest()` to validate the library after loading
- MQTT Specification: [MQTT 3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)

## Acknowledgments

Implements MQTT 3.1.1 protocol specification as defined by OASIS.
