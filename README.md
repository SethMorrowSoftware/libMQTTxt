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

**Honesty note:** everything in 2.12.0 is *verified statically; needs an OXT
pass.* The gates and protocol vectors above are green, but no line of 2.12.0 —
library or demo — has been observed on a running engine against a live broker.
That includes the demo's window: no gate here can prove a stack BUILDS, only
that its geometry is sound and its script is well-formed. The performance
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

### 2.12.0 (Current)

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
