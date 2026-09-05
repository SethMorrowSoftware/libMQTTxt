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
intact, QoS 1, each PUBACKed. So a 200 KB write completes on this engine
against THAT broker. **That does not close the original failure**, which was
against the local mosquitto at 192.168.1.104 - a different broker, not yet
re-run with the fix. Until it is, the mosquitto result stands as an
unexplained failure with a plausible cause, not a fixed one. If the re-run
fails again with `__writeSocket` reporting nothing, the cause is not a partial
write and this entry is wrong about it.

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

**Still untested:** the mosquitto 200 KB re-run (1.1); multi-connection (needs
`kCtHost2` set - the library keys connections by `host:port`); auto-reconnect
after a broker restart; persistent store; TLS end to end (1.2).
