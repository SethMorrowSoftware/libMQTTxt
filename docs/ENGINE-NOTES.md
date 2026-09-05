# ENGINE-NOTES.md - what OXT and real brokers actually do

Every entry here is a fact that cost this project something to learn. Not style,
not convention, not what a reference says: observed behaviour, with how we found
out and what it broke.

This file exists because OXT has no headless way to compile or run a
`.oxtstack`. Every gate in `tools/` is a stand-in for a compiler that cannot be
run, and this is the list of things no stand-in predicted.

## THE EVIDENCE RULE

Each entry carries a class, and the class is the point:

- **OBSERVED** - seen on a real engine, on a dated run. This is knowledge.
- **INFERRED** - derived from an observed failure but not directly seen. Usable,
  and explicitly weaker.
- **DOCUMENTED** - from a reference, never confirmed here. A claim, not a fact.

**Do not promote an entry between classes without a dated run.**

---

## 1. Sockets

### 1.1 A failed socket write does not throw
**OBSERVED 2026-09-05**, against mosquitto at 192.168.1.104:1883.

A 204800-byte QoS 1 PUBLISH was written. `mqttPublish` returned `OK` and logged
`Published: ... (204800 bytes, QoS 1)`. Nothing came back - not the echo, not
the PUBACK. **Nor did anything else come back for the rest of the session:** the
next PUBLISH went unacknowledged, the UNSUBSCRIBE that followed logged
`Unsubscribed from:` with no matching `Unsubscribe confirmed:`, and the run's
remaining stages all timed out.

That shape - writes reporting success while the inbound direction is silent - is
a partial write. The broker holds a fixed header promising N bytes, reads
everything sent afterwards as that packet's missing tail, and so never completes
a packet again. Neither side reports an error: the client sees clean writes and
a socket the engine still lists in `the openSockets`.

**What made it invisible:** all twelve of this library's socket writes were
`try / write ... to socket / catch`. LiveCode reports a socket write failure in
`the result`, so the catch caught nothing.

**Gate:** `tools/check-libmqttxt.py` refuses a bare `write ... to socket`;
everything routes through `__writeSocket`, which checks the throw AND `the
result`.

**Second run, 2026-09-05, against broker.hivemq.com with the check in place:**
the size ladder round-tripped 4 KB, 16 KB, 64 KB, 128 KB and 204800 bytes
intact, QoS 1, each PUBACKed. A 200 KB write completes on this engine against
that broker over the internet.

**Third run, same day, back against mosquitto at 192.168.1.104 - and this is
the one that settles it.** 4 KB, 16 KB and 64 KB round-tripped intact. Then:

    FAIL  large payload at 131072 bytes - ERROR: timeout

That is not the test's deadline; it is the library REPORTING the write failure
- the fix's first half, working. Everything after it confirms the mechanism
predicted above: the next PUBLISH went out and never came back, the UNSUBSCRIBE
logged no UNSUBACK, and the strengthened keep-alive check fired exactly as
designed - `a PINGREQ went out and no PINGRESP came back (lastPingTime still
1788649708812) - the link is dead inbound and only looks connected`. Diagnosis
CONFIRMED.

**Three things this run establishes:**

1. **The ceiling is path-dependent, not a library constant.** Mosquitto over a
   LAN: 65536 bytes passes, 131072 fails. hivemq over the internet: 204800
   passes. Whatever bounds it, it is not the library's framing - the same bytes
   go out either way.
2. **The failure is a `timeout` from a synchronous write.** `write ... to
   socket` without `with message` blocks until the data is written or `the
   socketTimeoutInterval` elapses (engine default 10000 ms). Why a 128 KB LAN
   write would take that long is NOT established - candidates are the
   engine's own write path being throughput-bound, or the broker refusing the
   size (mosquitto's `message_size_limit`) - and the conformance ladder now
   times every rung so the next run discriminates them: writes that grow with
   size and then hit the interval are the first; instant small rungs and one
   outright refusal are the second.
3. **Reporting the error was only half the fix.** The library returned `ERROR:
   timeout` and left the connection up with a corrupt output stream, so every
   later write was eaten as the tail of the broken packet. v2.12.2:
   `__failWrite` resets the connection the instant any write fails - the same
   response the PINGREQ path already had - and schedules a reconnect if one is
   enabled. A caller now gets `ERROR: timeout (connection reset)` and a
   `disconnected` state change, instead of a connection that answers nothing.

**Fourth run, same day, mosquitto again, v2.12.2 with per-rung write timing.**
The instrument answered the question it was built to answer:

    rung        write call   result
    4096 B         12 ms     ok
    16384 B        12 ms     ok
    65536 B        13 ms     ok
    131072 B       16 ms     ok      <- this size FAILED in run 3
    204800 B    10056 ms     ERROR: timeout (connection reset)

Then, for the first time: `ERROR writing PUBLISH: timeout - the stream may be
corrupt; resetting the connection`, `Auto-reconnect disabled` (not ticked), and
a clean `ABORT the connection dropped mid-run` - the v2.12.2 reset working as
designed instead of a connection that answers nothing.

**Two readings, both INFERRED from the timings:**

- **The write is buffer-bound, not throughput-bound.** 4 KB and 128 KB take the
  same 12-16 ms: the synchronous write is handing bytes to the kernel send
  buffer and returning the moment the kernel accepts them. It blocks only when
  the buffer is FULL, and then it blocks for the whole `socketTimeoutInterval`
  - which means the remote did not drain a single buffer's worth in ten
  seconds, on a LAN. Explanation 1 above (an engine write path too slow) is
  therefore WRONG: the path is fast right up to the cliff.
- **The ceiling is not a fixed broker limit.** 131072 bytes failed in run 3 and
  passed in run 4, same broker, same engine, same machine. A `message_size_limit`
  would refuse the same size every time. What varies run to run is the kernel's
  send-buffer size (TCP autotuning), which fits a cliff that moves.

**What is NOT yet established:** why the remote stops draining. The test
subscribes to its own topic, so every publish is echoed back over the same
socket - and a synchronous write blocks the engine's event loop, so nothing on
this side reads while it waits. Whether that pairing is a flow-control deadlock
(each side waiting for the other to read) is the leading candidate and is
UNPROVEN. The design decision this forces - whether the write path should stop
blocking the engine at all - is under review; see the entry that follows this
one when it lands.

**Still worth checking on the mosquitto side:** `message_size_limit` in
`mosquitto.conf` and `conf.d/`, if only to close that door formally.

### 1.2 `secure socket ... with verification` reports success on a plaintext port
**OBSERVED 2026-09-05.** `secure socket` was applied to a connection on port
**1883** - plaintext mosquitto, no TLS listener - and the library logged
`Socket secured with TLS (verified)`. No CONNACK ever followed.

A TLS handshake against a server speaking no TLS cannot succeed, so the success
this reports is not a verified channel. Whether the failure is deferred or
simply unreported is not established.

**Consequence, and it is a security one:** a caller cannot read "secured with
TLS (verified)" as evidence that anything was verified. The absence of a CONNACK
was the only signal that the connection was not usable.

**Not yet closed.** The library still reports what the engine tells it. Until
this is understood, treat TLS status as unverified regardless of what the log
says, and confirm a TLS connection reached CONNACK before trusting it.

### 1.3 At QoS 2, the echo arrives before our own handshake completes
**OBSERVED 2026-09-05**, against broker.hivemq.com.

Publishing at QoS 2 to a topic we are subscribed to puts TWO exchanges in flight
with independent packet IDs: ours (`PUBLISH 3 -> PUBREC 3 -> PUBREL 3 ->
PUBCOMP 3`, which clears the pendingAcks entry) and the broker's delivery back
to us (`PUBLISH 101 -> PUBREC 101 -> PUBREL 101`, which hands the message to
the callback). The log shows the order they actually interleaved:

    PUBREC received for packet 3
    PUBREL received for packet 101        <- our message is delivered HERE
    FAIL  the QoS 2 acknowledgment leg    <- checked pendingAcks at this instant
    PUBCOMP received for packet 3         <- and it drained one line later

**What it broke:** the conformance run asserted, at the moment of delivery,
that our acknowledgment leg had drained - and reported a correct library as
leaving an ack outstanding. **Gate:** none needed in the library, which is
right; the conformance stage now polls pendingAcks with a deadline instead of
asserting it on delivery.

---

## 2. What a run has established

### 2026-09-05, OXT + mosquitto (local) and broker.hivemq.com

**Green, and now OBSERVED rather than argued:**

- the library and demo compile and load; the demo builds its window
- `mqttInitialize` from `preOpenStack` - the embedded path
- CONNECT accepted by two independent brokers
- the no-quantifier `read from socket ... with message` streams inbound bytes
- byte-exact framing on live packets: **a 13-byte, 7-character UTF-8 payload
  round-tripped byte for byte**, which is exactly the case a character-counted
  Remaining Length gets wrong
- **all 256 byte values** survive a round trip, null included
- QoS 0, 1 and 2 round trips, with the acknowledgment legs drained, and QoS 2
  delivered exactly once
- unsubscribe removes the filter from the table

**Not established by the FIRST run, and it said otherwise:**

- **keep-alive.** The hold stage passed, and should not have. The library pings
  at 45s idle and only gives up 90s after the PINGREQ, so across a 100s hold a
  connection whose PINGRESP never came back is still marked connected. With the
  inbound path already dead from 1.1, that PASS proved nothing. The stage now
  requires `lastPingTime` to be 0 - the value `__parsePingResp` zeroes.
- **"nothing is delivered after unsubscribing."** Vacuous in that run: nothing
  was being delivered at all.

### Second run, 2026-09-05, OXT + broker.hivemq.com (v2.12.1)

13 passed, 1 failed, 1 skipped - and the one failure was the test's (1.3), not
the library's.

**Newly OBSERVED:**

- **keep-alive, properly this time.** 100s idle, `mqttIsConnected` still true
  AND `lastPingTime` back to 0: a PINGREQ went out at the threshold, a PINGRESP
  came back, `__parsePingResp` processed it, and the timer rescheduled. The
  token-routed timer chain works for one connection.
- **large payloads to 204800 bytes** round-trip intact and PUBACKed, on this
  broker - see 1.1 for why that does not yet close the mosquitto failure.
- **retained messages**: published with the flag, delivered live, then replayed
  to a fresh subscription after an unsubscribe/resubscribe, then cleared with a
  zero-length retained publish - which was itself delivered live as a 0-byte
  message, so **zero-length payloads frame correctly** too.
- **unsubscribe**, with UNSUBACK confirmed and nothing delivered afterwards -
  meaningful this time, because the inbound path was live throughout.
- **QoS 2 exactly-once**: delivered 1x, and the outbound handshake completed
  (PUBCOMP arrived; the test simply checked too early).

### Third run, 2026-09-05, OXT + mosquitto 192.168.1.104 (v2.12.1)

11 passed, 3 failed, 1 skipped. Two of the three failures are the SAME failure
seen through two instruments, and the third is its cascade - see 1.1. Nothing
new failed in the library; what this run did was confirm the diagnosis and show
that reporting a bad write is not the same as recovering from it.

### Fourth run, 2026-09-05, OXT + mosquitto 192.168.1.104 (v2.12.2)

9 passed, 2 failed, 0 skipped, and the run ABORTED cleanly at the 204800-byte
rung - which is the v2.12.2 reset doing its job. The two failures are one event
(the timed-out write) and its consequence (the abort). Nothing else regressed.
The write timings are in 1.1 and they change the diagnosis.

**Still untested:** whatever the write-path decision becomes (1.1);
multi-connection (needs `kCtHost2` set - the library keys connections by
`host:port`); auto-reconnect after a broker restart - the operator had it
unticked this run, so the reset stopped at "Auto-reconnect disabled"; persistent
store; TLS end to end (1.2).
