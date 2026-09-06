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

> **PLATFORM CAVEAT, added after the twelfth run.** Every observation in this
> entry - all ten runs of it - was made on **Kubuntu**. The reading it arrives
> at involves the kernel's TCP send-buffer autotuning, which Windows does
> differently, and the first Windows run used asynchronous writes and so never
> exercised the stall. **Whether any of this applies off Linux is untested.**
> `mqttSetAsyncWrites false` with `mqttSetWriteYieldMs 0` reproduces the
> pre-2.12.3 conditions exactly and would settle it in one run.

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

**The mechanism, INFERRED (2026-09-06).** Three independent analyses of the
four runs converged on one reading, and it is not the one this entry led with.
The engine's synchronous `write ... to socket` makes ONE non-blocking `send()`.
What fits in the kernel's TCP send buffer is accepted in memcpy time - the flat
12-16 ms. What does not fit is **never re-sent**: the wait loop runs to its
deadline, sets `the result` to `timeout`, and discards the unsent tail with the
socket still open. That is runs 1 and 3 exactly, and it is why the connection
looked alive while dead inbound.

Why this over the alternatives:

- **Not an echo deadlock** (each side blocked waiting for the other to read).
  A deadlock would be MORE likely over the slow hivemq path, which passed
  200 KB. And run 3's later small writes were accepted by the kernel, which a
  two-sided jam would have blocked too.
- **Not a broker limit.** Fixed limits refuse the same size every time; this
  ceiling moved.
- **Not a slow write path.** Flat timing to the cliff, zero progress after it.
  Raising `socketTimeoutInterval` to 20000 (onionxt's handshake value) would
  fail at 20056 ms instead of 10056.
- **The moving ceiling** is the kernel send buffer's free space under TCP
  autotuning: Linux starts `tcp_wmem` at 16 KB and grows it with connection
  history; macOS starts at 128 KB. It grows larger over a long-RTT internet
  path, which is why hivemq fitted 200 KB. **No single write size is safe on a
  cold connection** - a 64 KB publish can fail on a fresh socket.

Evidence class is INFERRED and stays there: the timings are observed, the
send() mechanism is recollection of engine source by an analysis whose file
reads were blocked by a harness fault, so it could not cite a line. It is the
reading that fits every observation, and the fix below is also the experiment
that tests it.

> **REFUTED by the seventh run, 2026-09-06. Everything from here to the sixth
> run below is kept as the record of a wrong turn, not as guidance.** The
> chunked write did not fix the mosquitto path, and the reason it did not is
> that this mechanism is not what is happening. Read on to "What the seventh
> run showed" for the current reading.

**The decision (v2.12.3): chunked synchronous writes.** `__writeSocket` now
hands the kernel no more than `gMQTTWriteChunkSize` bytes per `write` (default
16 KB, the smallest cold buffer either platform starts with), checking `the
result` after each. Between chunks the kernel drains the earlier ones to the
wire on its own - that is TCP, not the engine - so each chunk meets a buffer
with room. The conformance ladder now runs to 1 MB, well past any send buffer:

- **1 MB round-trips** - the mechanism above is confirmed, and fixed.
- **a chunk still blocks** - the peer genuinely stopped draining, the mechanism
  above is wrong, and the echo-deadlock reading comes back.

The error text now carries the measurement: `timeout after 131072 of 204800
bytes`, not `timeout`.

**Why not asynchronous writes** (`write ... with message`), which was the first
design put forward: the engine discards a queued write on `close socket` - two
candidate behaviours were identified and neither flushes reliably - and both
disconnect paths write DISCONNECT then close on the next line. An async
DISCONNECT would be dropped, the broker would see a bare TCP close, and **every
clean disconnect would publish the Last Will.** `mqttCleanupAll` at shutdown is
the starkest case: no event-loop turn ever follows it. Chunked sync keeps the
guarantee those paths rely on, changes nothing about what `OK` means, and stays
on the mechanism four runs have exercised. Async remains the fallback if the
1 MB rung fails.

**Still worth checking on the mosquitto side:** `message_size_limit` in
`mosquitto.conf` and `conf.d/`, if only to close that door formally.

**Sixth run, 2026-09-06, broker.hivemq.com:8883 over verified TLS, v2.12.4
with 16 KB chunking, `socketTimeoutInterval` 20000.** Every rung round-tripped
intact and was PUBACKed:

    rung          write call
    4096 B          12 ms
    16384 B         12 ms
    65536 B        119 ms
    131072 B       115 ms
    204800 B        14 ms
    524288 B       115 ms
    1048576 B      117 ms

Two readings, and they have to be kept apart. **OBSERVED:** a 1 MB PUBLISH goes
out as 64 chunked writes in about 120 ms and comes back byte-identical; the
write path carries a payload sixty times the old ceiling. **NOT established:**
that the chunking is what made it possible. This broker carried 200 KB in run 2
before chunking existed, and the path here was TLS, whose writes go through the
engine's OpenSSL layer rather than the plain `send()` the mechanism above
describes. The experiment that tests the mechanism is this ladder on the
mosquitto LAN path, in the clear, where a single write failed at 128 KB and
200 KB. Until that runs, the mechanism stays INFERRED, and the ladder's PASS
line says only what it saw - it used to append "so the chunked write is doing
its job" at 1 MB, and no longer does.

One more observation for whoever reads the next run: the write call took
either about 12 ms or about 115 ms, with no relation to size (64 KB slow,
200 KB fast). Two clusters, seven points, no explanation offered here - it is
recorded so the mosquitto run can be compared against it.

### What the seventh run showed, and what it refutes
**OBSERVED 2026-09-06**, mosquitto at 192.168.1.104:1883 in the clear, v2.12.4
with 16 KB chunking, `socketTimeoutInterval` 10000. Two conformance runs back
to back on one engine:

    run   rungs that passed          then
    a     4096, 16384, 65536         131072: timeout after 98304 of 131116 bytes
    b     4096, 16384, 65536, 131072 204800: timeout after 180224 of 204844 bytes

98304 is six 16 KB chunks. 180224 is eleven. **So the chunking worked exactly as
designed and the write stalled anyway** - six 16 KB writes were accepted in
about 13 ms, and the seventh blocked for the full ten seconds with zero bytes
of progress.

**That refutes the v2.12.3 mechanism.** "One non-blocking `send()` whose
unsent tail is discarded" predicts that a write small enough to fit always
succeeds. Six succeeded and the seventh did not, on the same socket, at the
same size, milliseconds apart. Nothing about the chunk was too big. The socket
became unwritable and stayed unwritable for ten seconds on a LAN, which means
the peer's receive window was shut: **the broker stopped reading us.**

**The current reading (INFERRED): we stopped reading first, and it is a
two-sided stall.** The conformance run subscribes to the tree it publishes to,
so from the first bytes of a large PUBLISH the broker is pushing the echo back
at us. A synchronous `write` never returns to the engine's event loop, so the
armed `read from socket` is never serviced, our receive buffer fills, the
broker's writes to us block, and a broker that cannot flush to a client stops
draining what that client sends. Both directions are then waiting on the other.
Chunking cannot help: it changes the size of each write, not the fact that
nothing is reading.

Why this fits the runs that passed, which is where the earlier reading went
wrong. Over the internet to hivemq the send buffer autotunes large (a long
round-trip path needs a big window), so the whole payload was accepted in one
go, the write returned, and the event loop got back to reading before anything
backed up. On a LAN the round trip is sub-millisecond, so the buffer stays
small AND the echo starts arriving almost immediately - the worst case for this
stall, and the only path where it has ever been seen. The ceiling moves between
runs because the buffer size does.

**Why it stays INFERRED.** Nothing here observed the broker's side. The
discriminating experiment is now the conformance run's first large stage
(`ctStageNoEcho`, ahead of the ladder): the same 1 MB payload published to a
topic *nothing is subscribed to*, judged by its PUBACK. No echo, no inbound
pressure. If that passes on the path where the ladder fails, the echo is the
cause. If it fails too, the echo is exonerated and the engine's write path is
the suspect again.

**The decision (v2.12.6): the chunked write yields between chunks.**
`__writeSocket` now runs `wait 0 milliseconds with messages` between chunks, so
the engine gets one turn of its event loop per 16 KB: the pending read
delivers, the receive buffer drains, the broker unblocks and resumes draining
us. The chunking stays - it is what bounds each blocking window and what makes
the error text a measurement - but the yield is the part that addresses the
stall. It also stops a megabyte publish from freezing the UI and starving the
keep-alive timers, which is worth having whichever way the experiment falls.

**The hazard the yield introduces, and the guard.** A yield runs application
code inside the write. A message callback that publishes would write a second
packet into the middle of this one - the exact stream corruption this whole
path exists to prevent. So a multi-chunk write takes a per-socket lock
(`gMQTTWriteLocks`, its own global, never the connection record, which
re-entrant code may delete) and a re-entrant write is REFUSED with an error
rather than interleaved; after each yield the socket is re-checked and a write
whose socket went away reports how far it got. Single-chunk writes - every
control packet, DISCONNECT included, and any publish that fits one chunk -
never yield, never lock, and are byte-for-byte the code that ran before.
`tools/check-libmqttxt.py` refuses `wait ... with messages` anywhere else in
the library.

**If the yield is not enough** (the ladder still stalls while the un-echoed
publish passes), the next candidate is asynchronous writes for large payloads
only, keeping DISCONNECT synchronous - which is the design the Last Will
objection below ruled out for ALL writes, and which that objection does not
actually reach when it is confined to publishes.

### The eighth run: the echo is exonerated too
**OBSERVED 2026-09-06**, mosquitto at 192.168.1.104:1883 in the clear, v2.12.6
(chunking AND the yield), `socketTimeoutInterval` 10000. The experiment above
ran, and it answered:

    FAIL  1048576 bytes to a topic nothing echoes back
          ERROR: timeout after 327680 of 1048619 bytes (connection reset)
          the write call took 10123ms

327680 is twenty 16 KB chunks. Twenty went out with a yield between each, and
the twenty-first blocked for the full ten seconds. **Nothing was subscribed to
that topic**, so there was no echo to back up, and the yield had been giving the
event loop its turn all along.

**So the echo hypothesis is dead, and so is the yield as a fix for this.** The
run before it killed the send-buffer hypothesis. What survives:

| reading | status |
| --- | --- |
| one `send()`, unsent tail discarded | refuted (run 7: six chunks, then a wall) |
| the broker blocked on an echo we were not reading | refuted (run 8: no echo, same wall) |
| the engine's plaintext write path stalls | **standing** |
| this broker, or this network path, stalls | **standing** |
| the engine needs elapsed time, not just a turn, to drain | **standing** |

The shape is consistent across all four failures: a burst of chunks accepted at
memcpy speed, then one chunk that makes zero progress for exactly the socket
timeout. The amount that gets through moves run to run - 98304, 180224, 327680 -
which still fits a buffer whose size varies and still rules out any fixed
per-packet limit the broker might impose.

**What v2.12.7 does about it, which is instrumentation and not a fix.** Two
experiments, because the remaining readings need different ones:

1. **A second broker, in the clear** (`ctStageAltWrite`, now the run's FIRST
   stage): the same megabyte published to broker.hivemq.com:1883 on its own
   connection, before anything else, judged by its PUBACK. It runs first
   precisely because a stall later aborts the run. If the engine writes a
   megabyte of plaintext to a broker across the internet without stalling, the
   engine's write path is exonerated and the local broker or the LAN path owns
   this. If it stalls there too, the write path is the suspect and TLS is the
   only shape that has ever carried this much.
2. **`mqttSetWriteYieldMs`**: the inter-chunk yield becomes a settable pause.
   Zero, the default, is a yield with no elapsed time. Setting it to 20 or 50
   tests whether the engine needs real time to push what it has accepted -
   without editing the library.

**And the observation this most needs is not on the client at all.** Nobody has
yet looked at what mosquitto does during the stall. Running it in the foreground
with `mosquitto -v`, or reading its log while the ladder runs, would say in one
line whether the broker is refusing the packet, hitting a limit, or simply not
being handed the bytes. That is a cheaper answer than any further inference from
this side, and it is the next thing to do.

### The ninth run: it is the engine's plaintext write, and no broker is involved
**OBSERVED 2026-09-06.** The probe ran. The same 1 MB publish, in the clear,
stalled against BOTH brokers in one run:

    broker                       written before the stall   of
    broker.hivemq.com:1883        65536  (4 chunks)         1048615
    192.168.1.104:1883           442368  (27 chunks)        1048619

Two brokers. Two networks - one LAN, one internet. Both plaintext. Both stall
after the socket timeout with the connection reset. **And the same engine, in
run 6, carried a megabyte to broker.hivemq.com over TLS in 117 ms.**

**So the broker is not the variable and the network is not the variable.
Plaintext is.** `message_size_limit` and every other broker-side explanation is
dead: hivemq's public broker and a local mosquitto do not share a limit, and
neither refuses a megabyte over TLS. This is the strongest result of the whole
investigation because it is a controlled comparison rather than an inference:
one variable changed, the failure followed it.

**The mechanism, and note that it is the ORIGINAL reading with the piece that
was missing.** The engine's plaintext write does one non-blocking send and does
not retry a socket that is momentarily full. v2.12.3 got that right and then
drew the wrong conclusion from it: chunking does not help, because chunks
written back to back **cost no elapsed time**. A yield of zero returns the loop
its turn and returns instantly, so the kernel's send buffer is filled far faster
than the wire drains it, and the first chunk that meets a full buffer times out.
Run 7's "six chunks then a wall" was never evidence against the send buffer - it
was the buffer filling in six chunks.

The byte counts are then simply how much fitted before that happened, which
explains every number this investigation has produced: they move with the path
(65536 over a slow internet link, 442368 on a LAN with a bigger autotuned
buffer), they move between runs on one path (autotuning), and they have no
relation to the payload size. TLS escapes it because the engine's TLS write goes
through a different path that does loop until the data is accepted.

Evidence class: the plaintext-versus-TLS split is **OBSERVED** - a controlled
comparison across two brokers. The send-and-do-not-retry mechanism inside the
engine remains **INFERRED**; it is the only reading left that fits, but nobody
here has read the engine's source.

**What v2.12.7's probe becomes (the pacing ladder).** If the cause is that we
write faster than the wire drains, the cure is to write slower, and the question
is only how much slower. The stage now republishes the same megabyte at an
increasing pause per chunk - 0, 5, 20, 50 ms - on a fresh connection each time,
and reports the smallest pause that gets a megabyte through. **A pause that
works stays set for the rest of the run**, so the ladder against the broker
under test becomes an independent second test of the same answer.

That measurement is what a real fix needs. A fixed pause cannot be right for
every path (a fast LAN wastes it; a slow uplink needs more), so the answer this
produces decides between a paced default and asynchronous writes, which
self-pace because the engine reports each chunk as it goes. Async is now the
favourite for the eventual fix; the pacing number tells us how much it is worth.

### The tenth run: pacing is the cure, and the number is measured
**OBSERVED 2026-09-06.** The ladder ran against broker.hivemq.com:1883 in the
clear, one megabyte per rung on a fresh connection:

    pause per chunk   result
    0 ms              timeout after 65536 of 1048615 bytes
    5 ms              timeout after 65536 of 1048615 bytes
    20 ms             timeout after 65536 of 1048615 bytes
    50 ms             PUBACKed, 3287 ms

**65536 bytes, three times, to the byte.** The stall point does not move with
the pause, only with the path - it is that path's send-buffer capacity, and the
pause decides whether the buffer is ever driven into it. That is the mechanism
confirmed by construction rather than inferred: **the write outruns the link,
and slowing it down fixes it.** Promote the pacing half of 1.1 to OBSERVED. The
send-and-do-not-retry behaviour inside the engine stays INFERRED - still nobody
has read the engine's source - but it is now the only reading with no
competitor.

**Then the pause was kept for the rest of the run, and the whole thing went
green against mosquitto on the LAN: 16 passed, 0 failed, 1 skipped**, ladder to
1 MB included, the un-echoed megabyte included, keep-alive across 100 s
included. The first fully green conformance run of the project.

**And the timings say exactly what the pause costs.** Every rung reports the
same throughput, on both brokers, on both networks:

    payload      chunks  pauses x 50ms   observed   pause is   real write work
    65536 B        4        150 ms        216 ms      69%          66 ms
    131072 B       8        350 ms        420 ms      83%          70 ms
    204800 B      13        600 ms        626 ms      96%          26 ms
    524288 B      32       1550 ms       1647 ms      94%          97 ms
    1048576 B     64       3150 ms       3289 ms      96%         139 ms

Actual write work is 26-139 ms whatever the size; the rest is the pause. The
"311 KB/s" every rung reports is not the network - it is 16384 bytes / 50 ms =
320 KB/s, the rate the pause permits, and nothing else. A megabyte's worth of
real writing took 139 ms, so **the LAN could carry this an order of magnitude
faster and the pause is throwing that away.**

**BUILT IN 2.13.0, and the argument for it is below.** The pacing default of
50 ms stays as the fallback path; asynchronous writes replace it as the default.
The design and its one risk are in section 4.

**Which is the argument for the eventual fix.** The required pause is set by the
slowest link an application uses: this operator's uplink sits somewhere between
320 KB/s (works) and 800 KB/s (fails), while their LAN is far quicker. One fixed
number cannot serve both, and a default must be safe, so v2.12.8 defaults to 50
and documents the cost. **Asynchronous writes are the right answer** - the
engine reports each chunk as it is actually written, so the next one goes when
the socket is ready, at exactly the link's rate with no guessing and no waste.
That is a real change: it needs a per-connection outbound queue so a control
packet cannot jump ahead of a queued publish, and it changes what `mqttPublish`
returning `OK` means from "written" to "queued". Worth doing, worth doing
carefully, and not worth bolting onto a run that just went green.

**Also OBSERVED this run:** a CONNECT to a routable address with nothing
listening produced `ERROR sending CONNECT: socket closed after 0 of 30 bytes` -
the engine accepted `open socket`, the peer closed, and the write check caught
it on the CONNECT itself with an exact byte count. The 2.12.1 write check
working on the one packet that had never exercised it.

### 1.4 A DISCONNECT written just before `close socket` may not reach the broker
**INFERRED (2026-09-06), not yet observed, recorded so it is not lost.**

Both disconnect paths write DISCONNECT (`0xE0 0x00`) and close the socket on the
next line. That is correct only because the write is synchronous - the kernel
has the bytes and `close(2)` sends FIN after them. But if UNREAD inbound data is
sitting in the receive buffer at `close(2)`, Linux and BSD send RST instead of
FIN and may discard unsent outbound data. The conformance run subscribes to its
own topic tree, so an echoed PUBLISH is often pending inbound at exactly the
moment `mqttDisconnect` runs - and a lost DISCONNECT is an unclean disconnect to
the broker, which then publishes the Last Will on what the client logged as a
clean exit.

The robust sequence, which MQTT 3.1.1 section 3.14.4 anticipates, is: write
DISCONNECT, then wait briefly for the broker's `socketClosed` (or drain pending
inbound) before closing locally. Not implemented: it changes `mqttDisconnect`
from immediate to deferred, and this is an inference with no observed LWT
misfire behind it. **The test that would settle it:** connect with a Will, hold
a second subscriber on the Will topic, run the ladder, disconnect - the second
subscriber must NOT receive the Will.

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

**Sixth run, 2026-09-06: TLS works where it should.** `secure socket ... with
verification` on broker.hivemq.com:**8883** - a real TLS listener with a
publicly signed certificate - logged `Socket secured with TLS (verified)`, then
a CONNACK, then a full conformance run over the encrypted channel: 14 passed, 0
failed, keep-alive across 100 s and the 1 MB rung included. **OBSERVED:** the
TLS path carries MQTT end to end against a valid certificate. **Still not
established:** that "verified" means verified. This run shows the success
message on a good certificate and the first run showed the same message on a
port with no TLS at all, so the message is consistent with both, and no bad
certificate has ever been presented to it. The test that would settle it:
verification ON against a broker whose certificate is self-signed and not in
the CA path - the connection must fail. Until then the rule above stands: trust
the CONNACK, not the log line.

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

### 1.5 Auto-reconnect: the back-off is observed, and it is unbounded
**OBSERVED 2026-09-06**, against broker.hivemq.com:8883 in the clear - the TLS
port without TLS, so the broker accepts the TCP connection, reads a plaintext
CONNECT, and closes.

With auto-reconnect ticked for the first time in six runs, every close
scheduled a retry:

    attempt   delay
    1          0.822 s
    2          2.233 s
    3          4.216 s
    4          8.202 s
    5         16.195 s
    6         29.86 s
    7 on      29.8 - 30.25 s

That is `min(2 ^ n, 30)` with +/- 0.25 s of jitter, as coded, and every attempt
met the same fate: `Socket connected`, `CONNECT packet sent`, `Socket closed`.
Some forty cycles later a manual Disconnect stopped the chain: `mqttDisconnect`
cancels the pending reconnect message and clears the flag, and no timer fired
afterwards (OBSERVED).

**Three things this establishes:**

1. **The reconnect path fires and re-arms.** Runs one to four never reached it
   (`Auto-reconnect disabled`). The socketClosed -> `__handleReconnect` ->
   timer -> `mqttReconnect` -> `open socket` loop works, with token routing
   and no double scheduling across forty cycles. What has NOT been observed is
   a reconnect that SUCCEEDS: every attempt here was to a port that refused
   it, so `__resubscribeAll` and the `success` callback have still never run
   on an engine. A broker restart on the plaintext port is the test.
2. **Retries are unbounded by design, and this is the case that shows the
   cost.** A broker that closes right after CONNECT every time is not an
   outage, and the library retried it every 30 s for as long as it was left.
   There is no attempt limit; the application decides when to stop, from the
   reconnect callback (`attempting` carries the count) by calling
   `mqttDisconnect`. Documented rather than changed: indefinite retry is the
   right default for the broker-restart case the feature exists for, and the
   deterministic refusals the broker can actually express - CONNACK codes 1, 4
   and 5 - already stop it.
3. **The log lied about the count.** Every line from the seventeenth on read
   `Scheduling reconnect attempt 17`, while the real counter kept climbing.
   `__handleReconnect` clamped the attempt count to 16 before raising 2 to it
   - correct, `2 ^ 40` is an overflow - and then logged the clamped value.
   **Fixed in v2.12.5:** the exponent is clamped, the count is reported. The
   callback was never affected; it reads the counter directly.

---

## 2. Lifecycle

### 2.1 An embedded library's preOpenStack initialisation did not take effect on a fresh engine
**OBSERVED 2026-09-06** (the FAIL line); **cause established by the eighth run:
the stack must be REOPENED, not have its script applied while open.**

Fifth engine run, on a stack named "Untitled 2" rather than the "Untitled 1"
of runs one to four. The boot self-check reported:

    FAIL  mqttInitialize ran: the keep-alive threshold is set (empty)
    FAIL  the embedded library's own self-test passes

Both lines are one fact: `gMQTTKeepAliveThreshold` was empty when the check ran,
so the demo's `preOpenStack` - the handler that calls `mqttInitialize` - had not
taken effect.

**How the log proves the globals were empty at start.** Every earlier run's log
begins `Callback target set to: stack "Untitled 1"`. This one begins `Log
callback set to: mdOnLog`. The missing first line is `mqttSetCallbackTarget`'s,
logged through `__mqttLog`, which stays silent while the log callback is unset.
In runs one to four it was ALREADY set - inherited from the operator's earlier
standalone-library session (`start using` -> `libraryStack` -> `mqttInitialize`)
in the same engine - so the line printed. **The embedded initialisation path had
therefore never actually run before this session; every earlier PASS on that
assertion was inherited, not earned** (INFERRED, and the reason the assertion
is now two lines).

**Why the cause is open.** `openStack` ran (the window built, the demo started,
the check ran), and the engine sends `preOpenStack` before `openStack` on every
open. So either the engine did not deliver it to a script pasted into an
already-open stack, or the stack reached `openStack` by some other route
(re-applying a script does not re-send either message; a manual `send openStack`
sends only that one). The operator's sequence for "Untitled 2" was not recorded.
Either way the conclusion is the same and the fix does not depend on it.

**What it would have broken.** Had the connection succeeded, the first inbound
byte would have reached `the number of bytes of tBuffer > gMQTTMaxBufferSize`
with an EMPTY limit. A number compared to empty is a STRING comparison in
xTalk, and "4" > "" is true - so the CONNACK would have been torn down as a
buffer overflow and the library could not have connected at all.

**Gate / fix (v2.12.4):** the library initialises itself. `mqttConnect` calls
`__ensureInit` (sentinel `gMQTTInitialized`), so nothing network-side ever runs
against empty globals whatever the host did at open time; `__maxBufferSize()`
guards the ceiling read like `__keepAliveThreshold()` already guarded its. The
demo calls `mqttInitialize` from `mdStart` as well as `preOpenStack`, and its
boot check asks the two questions separately: did `preOpenStack` fire (a
script-local marker), and is the library initialised. A stack that reaches
`openStack` without `preOpenStack` now says so in one line instead of failing
two unrelated-looking assertions.

**Sixth run, 2026-09-06, same stack "Untitled 2", same engine session.** Two
more facts:

- **Applying the v2.12.4 script to the open stack sent neither message.** The
  log has no second boot block: no preOpenStack, no openStack, no rebuild. That
  is the engine behaving as documented, and it means the fifth run's boot check
  DID reach openStack by some route while preOpenStack's work was missing. The
  route is still unrecorded. It also means the split boot check has still not
  run on a fresh engine. The demo header now says REOPEN in capitals.
- **The initialisation backstop carried the first real connection - INFERRED.**
  The TLS connect that followed reached `Connected successfully` and processed
  every packet of a full conformance run. The globals had been proved empty at
  the fifth run's boot check; nothing between there and this connect calls
  `mqttInitialize` (the 2.12.3 start handler did not, and the 2.12.4 one never
  ran because openStack never fired); so the only path that could have set the
  buffer ceiling before the first inbound byte is `__ensureInit` inside
  `mqttConnect`. INFERRED rather than OBSERVED because nothing logged it, which
  is why 2.12.5 logs the self-initialisation when it happens: `Library
  initialised on first use - mqttInitialize had not run`. The next fresh-engine
  run either shows that line (and the route question sharpens) or does not
  (and preOpenStack worked).

**Seventh run, 2026-09-06, back on stack "Untitled 1" (v2.12.4).** The split
assertion paid for itself:

    FAIL  preOpenStack fired before openStack (marker: empty)
    PASS  the library is initialised: keep-alive threshold set (0.75)

**preOpenStack did not fire on this stack either** - the second stack, in the
second engine session, to reach `openStack` with its `preOpenStack` marker
unset. Whatever the operator's edit-and-run sequence is, it is not delivering
`preOpenStack`, and that is now the expected shape rather than a surprise. The
library was initialised anyway. On this stack that proves nothing new about the
backstop (this is the engine session where `start using` had already run
`libraryStack`), which is why 2.12.5 logs the self-initialisation: the log line
is what will tell the two paths apart on a fresh engine.

**The lesson for the demo, not the library:** a one-file demo cannot rely on
`preOpenStack` for anything it needs in order to work. It initialises from
`mdStart` as well, and the library initialises itself; `preOpenStack` is now
belt, braces and a third fastener. The demo header says REOPEN in capitals for
the same reason.

**Eighth run, 2026-09-06: the question is closed.** On a REOPENED stack the
boot block read `14 passed, 0 failed`, marker included:

    PASS  preOpenStack fired before openStack (marker: true)
    PASS  the library is initialised: keep-alive threshold set (0.75)

Same stack, same engine, same script as the run that failed the marker
assertion; the only difference was reopening rather than applying the script to
an open stack. So the engine was never at fault. **Applying a script to an open
stack sends neither `preOpenStack` nor `openStack`**, which is also why the
sixth run produced no boot block at all - and the runs whose marker was empty
had reached `openStack` through some later route with `preOpenStack` never
delivered. Class this OBSERVED and the entry closed. The library's
self-initialisation stands anyway: an embedder will make this mistake, and the
failure it used to cause was a connection that could not receive a single byte.

### 2.2 The engine's `socketClosed` reaches the library
**OBSERVED 2026-09-06.** Connecting in the clear to broker.hivemq.com:8883 - the
TLS port - produced `Socket connected`, `CONNECT packet sent`, then
`Socket closed: broker.hivemq.com:8883` and `Auto-reconnect disabled`. The
broker closed the plaintext CONNECT; the engine sent `socketClosed`; the
library's `on socketClosed` dispatched to `mqttSocketClosed`, which found the
connection, tore it down and consulted the reconnect flag. The message that was
never wired in 2.11.9 (this library declared `mqttSocketClosed` and nothing
sent it) is now seen doing its job. The demo logs a hint when 8883 is used
without TLS, because the failure otherwise reads as a connection that simply
dropped.

---

## 3. What a run has established

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

### Fifth run, 2026-09-06, OXT, fresh engine session, v2.12.3

Boot self-check 11 passed, 2 failed - and the two failures were the boot check
catching exactly what it exists to catch: an uninitialised embedded library on
a fresh engine (2.1). No conformance run followed; the two connect attempts were
to hivemq's TLS port in the clear, which the broker closed and the library
handled correctly (2.2). The 1 MB rung of the write ladder has still not run.

**Still untested after this run:** the chunked write at 1 MB (1.1);
multi-connection (needs `kCtHost2` set - the library keys connections by
`host:port`); auto-reconnect after a broker restart - still unticked; persistent
store; TLS end to end (1.2); and now the v2.12.4 initialisation backstop on a
fresh engine (2.1).

### Sixth run, 2026-09-06, OXT + broker.hivemq.com:8883, same engine session, v2.12.4

Two phases. First, with the port still wrong (8883 in the clear) and
auto-reconnect ticked for the first time: some forty back-off cycles, then a
clean manual stop (1.5). Then TLS ticked: CONNACK, and the conformance run over
the encrypted channel - **14 passed, 0 failed, 1 skipped** (`kCtHost2` unset).
Every stage that had ever failed passed, and the ladder ran to 1 MB.

**Newly OBSERVED:**

- **TLS end to end** with verification on, against a real certificate (1.2).
- **the write path to 1 MB**, chunked, over TLS - sixty times the old ceiling
  (1.1; the mechanism claim stays INFERRED, and 1.1 says why).
- **auto-reconnect**: the back-off schedule, the 30 s cap, the jitter, and that
  a manual disconnect stops it (1.5). Not yet: a reconnect that succeeds.
- **the close-and-retry loop under repetition**: forty consecutive cycles with
  no orphan timer, no double schedule and no stale token firing - the timer
  work of 2.12.0 holding up.
- **QoS 2 interleaving** exactly as 1.3 describes, again.

**INFERRED:** the 2.12.4 self-initialisation carried the first connection
(2.1).

**Found:** the reconnect log line reported a clamped count (1.5; fixed in
2.12.5); the ladder's PASS text claimed a mechanism its path could not prove
(1.1; the line now reports only what it saw); the run's cleanup sent a second
UNSUBSCRIBE for a filter it had already dropped (legal, harmless, now
conditional).

**Still untested:** the 1 MB rung on the mosquitto LAN path in the clear - the
one that tests 1.1's mechanism; a reconnect that succeeds, with
`__resubscribeAll` (restart the broker during the hold stage, on 1883 with TLS
off); two connections at once (`kCtHost2`); the persistent store; the Will/RST
question in 1.4; certificate verification rejecting a bad certificate (1.2);
the split boot check on a fresh engine, and the route by which "Untitled 2"
reached openStack (2.1); and the self-initialisation log line added in 2.12.5.

### Seventh run, 2026-09-06, OXT + mosquitto 192.168.1.104:1883, v2.12.4

Two conformance runs back to back, 9 passed and 2 failed each, both aborting at
the large-payload ladder - **and this is the run that refuted the chunked-write
mechanism** (1.1). Chunking behaved exactly as designed and the stall happened
anyway: six 16 KB chunks out and the seventh blocked for ten seconds in the
first run, eleven chunks and the twelfth in the second.

**What it establishes:**

- **The v2.12.3 diagnosis was wrong** (1.1). A 16 KB write that succeeds six
  times and then stalls for ten seconds is not a write that was too large for
  the send buffer. The current reading is a two-sided stall, INFERRED, with the
  experiment now in the run.
- **Everything before the ladder passes on this path**, twice, deterministically:
  SUBSCRIBE, QoS 0/1/2 with their ack legs, exactly-once, UTF-8, all 256 byte
  values. The mosquitto path is not fragile; one specific thing on it is broken.
- **The failing size still moves** - 131072 in the first run, 204800 in the
  second, same broker, same engine, minutes apart. Consistent with a send buffer
  that autotunes, and inconsistent with any fixed broker limit; `message_size_limit`
  is now effectively ruled out as well as formally unchecked.
- **`__failWrite` and the abort path are solid**, run twice more: the error
  carries the byte count, the connection is reset rather than left corrupt, and
  the run stops with `ABORT` instead of printing unearned PASSes below it.

**Newly OBSERVED in the boot check:** preOpenStack did not fire on "Untitled 1"
either (2.1). Two stacks, two sessions, same shape.

**What v2.12.6 does about it:** the write yields to the event loop between
chunks, under a re-entrancy lock, and the conformance run gets a stage that
publishes 1 MB to a topic nothing echoes back - the experiment that tells the
echo stall apart from a broken write path. Both are in the next run.

### Thirteenth run, 2026-09-06, OXT on Windows 11 + broker.hivemq.com:8883 over TLS, v2.13.0

**16 passed, 0 failed, 1 skipped**, boot check 14 of 14. TLS and asynchronous
writes together for the first time, and the first run whose timings are
measurements rather than poll intervals: **a megabyte round-trips in 522 ms**
against the 3150 ms of pure pausing 2.12.8 imposed. Detail in section 4.

**Found:** the pacing ladder measured nothing - it set a pause four times while
asynchronous writes, which never read it, were on. Its "no pacing needed on this
path" was unearned. The stage now turns asynchronous writes off for its own
duration and restores them however it exits, which also makes it the only
engine exercise the synchronous fallback gets.

### Twelfth run, 2026-09-06, OXT on WINDOWS 11 + broker.hivemq.com:1883, v2.13.0

**15 passed, 0 failed, 2 skipped - the asynchronous write path's first engine
run, and the project's first run on anything but Kubuntu.** Both worked. The
detail is in section 4; the headline is that the queue keeps order on a real
engine and the 3150 ms pacing floor is gone.

**Newly OBSERVED:**

- **Asynchronous writes end to end.** Nineteen packets interleaved with
  PINGREQs, SUBSCRIBEs and UNSUBSCRIBEs across a ladder up to 1 MB, every one
  acknowledged in sequence, no corruption and nothing discarded.
- **Windows 11.** No platform-specific behaviour anywhere - framing, timer
  routing, keep-alive and the QoS 2 interleaving of 1.3 all identical to
  Kubuntu.
- **A megabyte round trip in under 765 ms**, against a floor of 3150 ms that
  2.12.8's pacing imposed arithmetically.

**Found in the harness:** the stages were reporting their own poll interval as
throughput - every duration a whole number of 250 ms ticks, and "15 KB/s" for a
4 KB payload the synchronous runs carried in 12 ms. Arrival times are now
stamped in the message callback, and the two stages that cannot stamp say
"within Nms, an upper bound" and print no rate (section 4).

**Not tested here:** the pacing ladder skipped (`kCtAltHost` was the broker
under test), multi-connection skipped (`kCtHost2` empty), and `preOpenStack`
did not fire because the script was applied to an open stack rather than
reopened - 2.1 behaving as documented.

### Eleventh session, 2026-09-06, four runs against mosquitto by two addresses, v2.12.7

Four runs in one engine session, ending at **17 passed, 0 failed, 1 skipped -
the most complete run of the project.** The operator drove the broker through
two spellings (`192.168.1.104` and `127.0.0.1`), which is what finally let the
last untested stage run.

**Newly OBSERVED, and it is the one that had never run:**

- **Two simultaneous connections, with independent keep-alive chains.** The
  second connection was opened to `192.168.1.104:1883` while the run was
  connected to `127.0.0.1:1883` - the same mosquitto, two keys, because the
  library keys a connection by `host:port`. Then both were held idle for 100 s
  at DIFFERENT keep-alive intervals, 30 s against the dashboard's 60 s:

      PASS  a second connection is live alongside the first
      PASS  the connection survived 100s idle AND a PINGRESP was received
      PASS  the SECOND connection also survived - the two keep-alive chains
            ran on their own schedules

  **This is the timer-token work of 2.12.0 proved on an engine.** Before that
  fix a single delayed message served every connection, so whichever timer fired
  first pinged both and the other was serviced on a schedule that was not its
  own. Two chains on two intervals surviving 100 s each is exactly the
  observation that could not be made until now, and it is the last stage of the
  conformance button to go green.
- **Repeatability.** The whole suite ran green three times over in one session,
  against two addresses, with packet IDs continuing across runs on a reused
  connection (21, 22, 23... in the last run) - so the packet-ID allocator does
  not restart or collide when a connection outlives a run.

**Found, and it is a defect in the harness rather than the library.** A run
cancelled mid-hold by a second click printed:

    -- run cancelled by a second click --
          15 passed, 0 failed, 1 skipped - RUN FINISHED
          conformance run GREEN against 127.0.0.1:1883

**Zero failures is not a pass when the stages that would have failed never
ran.** This file's own header promises a report that says when it is not
finished, and `ctFail` already carries that lesson one level up in its cascade
NOTE; the cancel path was the hole in it. `ctFinish` now records WHY a run
stopped and prints `RUN NOT FINISHED (<reason>)` with the stage it died on,
never GREEN. The abort path shares the mechanism.

**Also found:** the pacing ladder skipped in three of these runs because
`kCtAltHost` had been set to the same address the run was using, and one
connection per `host:port` is all the library can hold. That is a real
constraint, so the skip message now names the remedy - another spelling of the
same broker, which is exactly the trick the operator had already used for
`kCtHost2`. **The LAN pacing figure is therefore still unmeasured**; every
50 ms value in these runs came from the hivemq ladder in the first run and
persisted in the global for the rest of the session.

**Worth knowing for the next run:** these were all v2.12.7, whose default pause
is 0. The runs only worked because the first ladder set 50 ms and the global
survived. **On a fresh engine v2.12.7 would fail the large payloads again** -
v2.12.8's default of 50 is what makes that survive a restart, and it has not
been run yet.

### Tenth run, 2026-09-06, OXT + broker.hivemq.com then mosquitto, v2.12.7

**16 passed, 0 failed, 1 skipped - the first fully green conformance run.** The
pacing ladder found 50 ms per chunk, kept it, and every stage that had ever
failed then passed against the LAN broker (1.1).

**Newly OBSERVED:**

- **Pacing is the cure.** 0, 5 and 20 ms all stalled at exactly 65536 bytes;
  50 ms carried a megabyte. The stall point is the path's buffer; the pause
  decides whether it is ever filled.
- **The whole protocol surface, green in one run:** SUBSCRIBE/SUBACK, QoS 0/1/2
  with ack legs and exactly-once, UTF-8 and all 256 byte values, a megabyte both
  echoed and un-echoed, the full size ladder, retained replay and clear,
  unsubscribe with the negative check, and keep-alive across 100 s idle.
- **What the pause costs**, from the per-rung timings: 94-96% of the elapsed
  time on large payloads. Real write work is 26-139 ms regardless of size.
- **The write check on CONNECT**: a connect to a routable address with nothing
  listening reported `socket closed after 0 of 30 bytes`.

**Not tested, still:** `kCtHost2` was empty so multi-connection skipped again;
the persistent store; a reconnect that succeeds; certificate verification
refusing a bad certificate. `preOpenStack` did not fire because the script was
applied to an open stack rather than reopened, which is 2.1 behaving as
documented.

### Ninth run, 2026-09-06, OXT + mosquitto AND broker.hivemq.com, both :1883, v2.12.7

9 passed, 3 failed - and the three failures are one finding, which is the
finding this investigation was looking for. **A large plaintext write stalls
against two different brokers on two different networks, while TLS to one of
those brokers carries the same megabyte fine** (1.1). The variable is plaintext,
not the broker, not the LAN, not the echo, not the payload size.

**Newly OBSERVED:**

- **The controlled comparison itself.** Nothing else in this project has
  isolated a cause this cleanly: same engine, same payload, same run, two
  brokers, one variable.
- **The boot self-check green again, 14 of 14**, on a reopened stack - twice
  running now, so 2.1 stays closed.
- **`mqttSelfTest` inside the engine: 9 passed, 0 failed** at 2.12.7, printed
  from the demo's button.
- **The write-path failure is clean every time.** Five runs of `__failWrite` now:
  the byte count is reported, the connection is reset rather than left holding a
  half-packet, and the run aborts instead of printing unearned passes.

**What v2.12.7 leaves for the tenth run:** the probe becomes a pacing ladder
(0, 5, 20, 50 ms per chunk) that measures the smallest pause a megabyte needs,
and keeps a working pause set for the rest of the run so the ladder retests it.
The mechanism says pacing should work; the number decides whether a paced
default or asynchronous writes is the fix worth building.

### Eighth run, 2026-09-06, OXT + mosquitto 192.168.1.104:1883, v2.12.6

9 passed, 2 failed, and **the experiment answered: the echo is exonerated**
(1.1). A 1 MB publish to a topic with no subscriber stalled after twenty of
sixty-four chunks, with the yield in place. Two hypotheses are now refuted and
three stand; v2.12.7 ships the instruments that separate them rather than a
third guess at a fix.

**Also newly OBSERVED, and this one is green:**

- **The boot self-check passed 14 of 14 on a reopened stack**, `preOpenStack`
  marker included - the first fully clean boot block of the project (2.1). Two
  runs had shown that assertion failing; reopening the stack rather than
  applying the script to an open one is what changed, which confirms the demo
  header's instruction and closes the open question in 2.1.
- **The yield did not break anything.** Every stage before the large payloads
  passed exactly as before: SUBSCRIBE, QoS 0/1/2 with their ack legs,
  exactly-once, UTF-8, all 256 byte values, with the dashboard's own
  subscription live alongside the run. The re-entrancy lock cost nothing
  observable.
- **`__failWrite` and the abort path**, for the fourth and fifth time.

---

## 4. The asynchronous write path (2.13.0)

**DESIGNED FROM RUNS 7-10, NOT YET RUN ON AN ENGINE.** Everything here is
`verified statically; needs an OXT pass`. The gates behind it are
`tools/test-write-queue.py` (the ordering property, nine cases) and rule 13 of
`tools/check-libmqttxt.py` (only the pump may start an asynchronous write).

### Why

Section 1.1 ends with a measured cure and a bad bargain: a fixed pause per chunk
keeps a large write inside the link's drain rate, and the tenth run showed that
pause accounting for 94-96% of a megabyte's elapsed time. The pause has to be
sized for the slowest link an application will ever use, so a LAN pays for an
uplink it never touches.

The engine will instead report each write as it completes. A chunk started only
on that report cannot outrun anything: the queue drains at exactly the rate the
socket accepts. That is the same cure with the guesswork removed.

### The shape

One queue per socket, in its own global - never in the connection record, since
application code running during a callback can call `mqttCleanupAll`, and
writing a key back into a deleted record recreates it as a zombie the keep-alive
sweep walks forever (the lesson `__parseIncomingData` already carries).

    __writeSocket        picks the path; every caller still uses only this
      __enqueueWrite     appends, then pumps
      __writeSocketSync  the 2.12.8 paced path, kept as the fallback
    __pumpWriteQueue     starts ONE chunk if none is in flight
    mqttSocketWriteDone  the engine's report; advances and pumps again
    __dropWriteQueue     teardown, and before any synchronous DISCONNECT

### The one risk, and what holds it

**Two writes in flight on one socket interleave on the wire.** A PINGREQ started
while a megabyte of PUBLISH is half-written lands *inside* that publish; the
broker's framing desynchronises and never recovers, and neither side reports an
error. It is the same corruption a stalled synchronous write used to cause,
reached from the opposite direction.

What prevents it is that **every** packet goes through the queue - not just the
large ones - and the queue is strictly FIFO with at most one chunk outstanding.
There is no direct path. Rule 13 of the static gate enforces that
`__pumpWriteQueue` is the only caller of `__writeSocketAsync`, so the guarantee
cannot be eroded by a later change that "just needs to send a PINGREQ now".

Two subtler cases the model checks:

- **An enqueue during a write.** A message callback that publishes runs while a
  chunk is in flight. It appends and returns; the packet waits its turn.
- **`mqttSetWriteChunkSize` called between a write and its completion.** The
  completion must advance by the size actually written, so the pump records it
  in `inflight` rather than letting the completion recompute it. Recomputing
  leaves `pos` on the wrong byte and sends the packet's tail from the wrong
  offset - silently. This was a real defect in the first draft, caught by
  reading rather than by a run, and it is now a test case.

### What it changes for a caller

`mqttPublish` returning `OK` means **queued**, not written. A write that fails
after that point cannot be returned to the caller, so it arrives the way any
mid-connection failure does: the state-change callback with `disconnected` and a
reason. For QoS 1 and 2 the acknowledgment was always the real proof of
delivery; for QoS 0 the guarantee never existed. `mqttGetQueuedBytes` reports
what is still owed to a socket.

### Backpressure

An application that publishes faster than its link can carry would grow the
queue until the engine runs out of memory, and that failure lands nowhere near
its cause. `__enqueueWrite` refuses a packet that would take the queue past
`__maxBufferSize()` - the same ceiling that guards the inbound direction.

### DISCONNECT stays synchronous

Both teardown paths drop the queue and write DISCONNECT with
`__writeSocketSync`. A queued DISCONNECT would be discarded by the `close
socket` on the next line, the broker would see a bare TCP close, and **every
clean disconnect would publish the Last Will** (1.4). `mqttCleanupAll` is the
starkest case: no turn of the event loop ever follows it, so a queued write
would never run at all. Anything still queued at that moment is abandoned
deliberately and logged; a QoS 1 or 2 message among it is still in `pendingAcks`.

### The twelfth run: it works, on Windows, first time
**OBSERVED 2026-09-06**, OXT on **Windows 11** - the first run of this project
on anything but Kubuntu - against broker.hivemq.com:1883 in the clear.
**15 passed, 0 failed, 2 skipped.**

Every stage that has ever run passed, with the asynchronous path carrying all of
it: SUBSCRIBE/SUBACK, QoS 0/1/2 with their acknowledgment legs and
exactly-once, UTF-8, all 256 byte values, a megabyte echoed and a megabyte
un-echoed, the whole size ladder, retained replay and clear, unsubscribe with
its negative check, and keep-alive across 100 s idle.

**What this establishes:**

- **The queue keeps order on a real engine.** That was the whole risk: a
  control packet overtaking a half-written publish desynchronises the broker's
  framing silently, and it would show up as a stalled or nonsensical stage
  rather than a slow one. Nineteen packets went out interleaved with PINGREQs,
  SUBSCRIBEs and UNSUBSCRIBEs across a megabyte-scale ladder, and every one was
  acknowledged in sequence. No `Discarding N queued bytes`, no queue-full
  refusal, no corruption.
- **The write call is an enqueue, as designed.** 11-14 ms for every payload
  from 4 KB to 1 MB, where 2.12.8 took 3289 ms for the megabyte.
- **The pacing floor is gone, and this part is arithmetic rather than
  measurement.** Under 2.12.8 a megabyte is 64 chunks, so 63 pauses of 50 ms:
  **3150 ms of pure sleeping before any I/O, on any platform.** The same
  payload now completes, round trip, in under 765 ms. The comparison holds
  even though the platform changed, because the floor was never a property of
  the machine.
- **The library runs on Windows.** No platform-specific behaviour appeared
  anywhere: same framing, same timer routing, same keep-alive, same QoS 2
  interleaving described in 1.3.

**FOUND, and it is the instrument rather than the library.** The run reported
throughputs that are not measurements:

    4096 B      262ms   "15 KB/s"
    16384 B     513ms   "31 KB/s"
    204800 B    261ms   "766 KB/s"
    1048576 B   765ms   "1339 KB/s"

Every one of those durations is within 15 ms of a whole number of `kCtTickMs`
(250 ms) - 1.05, 2.05, 1.04, 3.06 ticks. **The stages were measuring their own
poll interval.** A 4 KB round trip did not take 262 ms; it took something under
262 ms, and dividing by that produced "15 KB/s" for a payload the synchronous
runs carried in 12 ms. Reporting a rate derived from a polled duration is the
same error as reporting a PASS that was never earned, and this file's whole
convention is against it.

**Fixed:** `ctOnMessage` now stamps the arrival time the moment a message
lands, so the echo-based ladder reports a true millisecond round trip. The two
PUBACK-only stages have no callback to stamp - nothing fires when a PUBACK
arrives - so they now say `PUBACKed within Nms (polled every 250ms, so an upper
bound)` and print **no rate at all**. `ctRate` carries a comment saying it may
only be called on a measured duration.

**Still not known, and Windows makes it cheap to find out:** whether the
plaintext write stall of 1.1 exists on this platform at all. Every observation
of it was on Kubuntu, and the reading involves the kernel's send-buffer
autotuning, which Windows does differently. `mqttSetAsyncWrites false` plus
`mqttSetWriteYieldMs 0` reproduces the pre-2.12.3 conditions exactly; if the
ladder then sails through on Windows, the stall is Linux-specific and 1.1 needs
saying so.

### The thirteenth run: real numbers at last, and a stage that measured nothing
**OBSERVED 2026-09-06**, OXT on Windows 11, **over verified TLS** to
broker.hivemq.com:8883, v2.13.0 asynchronous. **16 passed, 0 failed, 1 skipped**,
and the boot self-check green at 14 of 14 on a properly reopened stack.

**The timing fix worked, and these are measurements rather than poll intervals:**

    payload      round trip   incl. one round trip
    4096 B          104 ms      38 KB/s
    16384 B         284 ms      56 KB/s
    65536 B         286 ms     224 KB/s
    131072 B        295 ms     434 KB/s
    204800 B        234 ms     855 KB/s
    524288 B        319 ms    1605 KB/s
    1048576 B       522 ms    1962 KB/s

Not one of those is a multiple of the 250 ms tick, which is what the previous
run's numbers all were. **A megabyte now round-trips in 522 ms**, against
2.12.8's 3150 ms of pure pausing before any I/O. The write call stayed at
10-13 ms for every size.

The small rungs measure LATENCY, not throughput: 104 ms for 4 KB is one round
trip to a broker on the internet, and "38 KB/s" is that latency expressed as a
rate. Only the large rungs approach the link's real speed. The ladder now says
`incl. one round trip` so the two are not read as the same quantity.

**Also newly OBSERVED:** TLS carrying the asynchronous path end to end, which
is TLS and async together for the first time; and a cold-connection effect worth
recording - the ladder's fresh connection PUBACKed a megabyte within 2762 ms
while the same payload on the warm run connection took within 763 ms.

**FOUND, and it is the sharpest instrument defect yet: the pacing ladder
measured nothing at all.** It ran, printed a PASS on its first rung, and said:

    PASS  1048576 bytes ... at a 0ms pause per chunk
          NOTE: no pacing needed on this path - the engine wrote a megabyte
          of plaintext straight through.

**Neither claim was tested.** Since 2.13.0 the library writes asynchronously by
default, and the asynchronous path never reads `gMQTTWriteYieldMs` - the stage
set the pause four times and changed nothing. Every rung was identical; rung one
passed because asynchronous writes work, which the rest of the run already
showed. Worse than useless: a future reader would have taken "no pacing needed
on this path" as evidence about the synchronous path, and it is evidence about
nothing.

**Fixed:** the stage now turns asynchronous writes OFF for its own duration and
restores the previous settings however it exits - including from `ctFinish`, so
a cancelled run cannot leave the library on the other write path for the rest of
the session. That makes the ladder a real measurement again, and it also makes
it **the only exercise the synchronous fallback gets on an engine**, which was
listed below as still untested.

**So the Kubuntu-versus-Windows question in 1.1 is still open**, and the fixed
ladder is what will answer it: with asynchronous writes off and a 0 ms pause it
reproduces the pre-2.12.3 conditions exactly. If a megabyte goes straight
through on Windows, the stall is Linux-specific.

### What the next run has to show

1. **The conformance run still passes**, in the same shape as run 11's 17 of 17.
   Ordering is the risk; a broken queue shows up as a stalled or nonsensical
   stage, not as a slow one. **Met by the twelfth run** - 15 of 15 on Windows.
2. **The ack times.** Under 2.12.8 every rung reported ~311 KB/s because the
   pause set it. If the queue works, the megabyte's time-to-PUBACK should fall
   towards the ~139 ms of real write work the tenth run measured. **Met, with a
   caveat the run itself exposed:** a megabyte round-trips in under 765 ms
   against 2.12.8's 3150 ms floor of pure sleeping, but the finer figures were
   quantised to the poll interval and are now measured properly rather than
   inferred from a poll.
3. **`mqttSetAsyncWrites false`** must still behave like 2.12.8. If asynchronous
   writes misbehave, that one line is the way back without pinning a version.
   **Still not exercised on an engine** - the thirteenth run was supposed to,
   and the stage that would have done it was measuring nothing (above). The
   fixed pacing ladder now drives it on every run.
