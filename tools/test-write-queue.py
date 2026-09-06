#!/usr/bin/env python3
"""The outbound write queue's ordering guarantee, proved offline.

WHY THIS EXISTS. libMQTTxt 2.13.0 writes every packet asynchronously: a packet
is appended to a per-socket queue and written one chunk at a time, each chunk
started only when the engine reports the previous one done. The whole design
rests on ONE property - that the bytes reaching the socket are exactly the
packets, concatenated, in the order they were handed over. Break it and a
PINGREQ lands inside a half-written PUBLISH, the broker's framing desynchronises
and never recovers, and nothing on either side reports an error. That is the
failure this library has spent ten engine runs learning to avoid.

An engine cannot be run here, so the queue's state machine is transcribed into
Python and driven through the sequences that would break it: completions
interleaved with enqueues, an enqueue arriving DURING a completion (which is
what a message callback that publishes does), the chunk size changing mid-flight,
and a queue dropped under a socket that went away. The bytes on the simulated
wire are then compared against what was handed over.

A transcription can drift from its original, so the last check reads
libMQTTxt.oxtstack and asserts the lines this model depends on are still there.
"""

import os
import random
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE = os.path.join(os.path.dirname(HERE), "libMQTTxt.oxtstack")


class WriteQueue:
    """__enqueueWrite / __pumpWriteQueue / mqttSocketWriteDone, transcribed.

    `wire` is what the socket received, in order. `open` models
    __isSocketOpen.
    """

    def __init__(self, chunk_size=16384, limit=5242880):
        self.head = 1
        self.tail = 0
        self.pos = 1
        self.bytes = 0
        self.inflight = 0
        self.busy = False
        self.items = {}
        self.chunk_size = chunk_size
        self.limit = limit
        self.wire = b""
        self.open = True
        self.dropped = False
        self.on_write = None          # fires during a write, like a callback

    # --- __enqueueWrite ----------------------------------------------------
    def enqueue(self, packet):
        if len(packet) == 0:
            return ""
        if not self.open:
            return "the socket is not open"
        if self.bytes + len(packet) > self.limit:
            return "the outbound queue is full"
        self.tail += 1
        self.items[self.tail] = packet
        self.bytes += len(packet)
        self.pump()
        return ""

    # --- __pumpWriteQueue --------------------------------------------------
    def pump(self):
        if self.dropped:
            return
        if self.busy:
            return
        if self.head > self.tail:
            return
        if not self.open:
            self.drop()
            return

        item = self.items[self.head]
        end = min(self.pos + self.chunk_size - 1, len(item))
        self.busy = True
        self.inflight = end - self.pos + 1
        chunk = item[self.pos - 1:end]
        self.wire += chunk
        if self.on_write:
            self.on_write(self)

    # --- mqttSocketWriteDone ----------------------------------------------
    def complete(self):
        if self.dropped:
            return
        self.busy = False
        if self.head > self.tail:
            return

        item = self.items[self.head]
        sent = self.inflight
        if sent < 1:
            return
        self.inflight = 0
        self.bytes -= sent
        self.pos += sent

        if self.pos > len(item):
            del self.items[self.head]
            self.head += 1
            self.pos = 1
        self.pump()

    # --- __dropWriteQueue --------------------------------------------------
    def drop(self):
        self.dropped = True
        self.items = {}
        self.bytes = 0
        self.busy = False

    def idle(self):
        return self.head > self.tail and not self.busy


def drain(q, limit=200000):
    """Run completions until the queue is idle."""
    for _ in range(limit):
        if q.idle():
            return
        q.complete()
    raise AssertionError("queue never drained - the pump is not advancing")


def case_fifo_single():
    q = WriteQueue(chunk_size=64)
    packets = [b"A" * 10, b"B" * 200, b"C" * 1, b"D" * 64, b"E" * 65]
    for p in packets:
        assert q.enqueue(p) == ""
        drain(q)
    assert q.wire == b"".join(packets), "bytes are not the packets in order"
    assert q.bytes == 0, "byte counter did not return to zero: %d" % q.bytes


def case_fifo_interleaved():
    """Enqueues arriving while earlier packets are still draining."""
    rng = random.Random(20260906)
    for trial in range(300):
        q = WriteQueue(chunk_size=rng.choice([1, 7, 64, 1024]))
        packets = []
        pending = 0
        for _ in range(rng.randrange(1, 12)):
            p = bytes([rng.randrange(256)]) * rng.randrange(1, 500)
            packets.append(p)
            assert q.enqueue(p) == ""
            pending += 1
            # Complete a random number of chunks before the next enqueue.
            for _ in range(rng.randrange(0, 6)):
                if not q.idle():
                    q.complete()
        drain(q)
        assert q.wire == b"".join(packets), (
            "trial %d: interleaved enqueues reordered the stream" % trial)
        assert q.bytes == 0


def case_enqueue_during_completion():
    """A message callback that publishes, which is the re-entrant case."""
    q = WriteQueue(chunk_size=16)
    big = b"P" * 100
    late = b"Q" * 30
    fired = []

    def publish_from_callback(queue):
        # Fires while a chunk is in flight - the moment a callback would run.
        if not fired:
            fired.append(True)
            assert queue.enqueue(late) == ""

    q.on_write = publish_from_callback
    assert q.enqueue(big) == ""
    drain(q)
    assert fired, "the re-entrant enqueue never ran; the case proved nothing"
    assert q.wire == big + late, (
        "a packet queued during a write jumped ahead of the one in flight")


def case_chunk_size_changes_mid_flight():
    """mqttSetWriteChunkSize called between a write and its completion.

    The completion must advance by what was actually written. Recomputing it
    from the current chunk size leaves `pos` on the wrong byte and sends the
    rest of the packet from the wrong offset.
    """
    packet = b"".join(bytes([i % 256]) for i in range(1000))
    q = WriteQueue(chunk_size=100)
    assert q.enqueue(packet) == ""
    q.chunk_size = 37          # changed while the first chunk is in flight
    q.complete()
    q.chunk_size = 250
    drain(q)
    assert q.wire == packet, "a mid-flight chunk-size change corrupted the packet"


def case_backpressure():
    q = WriteQueue(chunk_size=1024, limit=1000)
    assert q.enqueue(b"x" * 900) == ""
    err = q.enqueue(b"y" * 200)
    assert "full" in err, "the queue accepted more than its limit: %r" % err
    drain(q)
    assert q.wire == b"x" * 900, "the rejected packet reached the socket anyway"


def case_socket_closes_mid_packet():
    q = WriteQueue(chunk_size=16)
    assert q.enqueue(b"Z" * 100) == ""
    q.complete()
    written = len(q.wire)
    q.open = False
    q.complete()               # pump finds the socket gone
    assert q.dropped, "the queue kept writing into a closed socket"
    assert len(q.wire) == written + 16 or len(q.wire) == written, (
        "bytes went out after the socket closed")
    assert q.enqueue(b"more") == "the socket is not open"


def case_empty_packet():
    q = WriteQueue()
    assert q.enqueue(b"") == ""
    assert q.wire == b"" and q.idle()


def case_spurious_completion():
    """A completion for a queue that is already empty must change nothing."""
    q = WriteQueue(chunk_size=8)
    assert q.enqueue(b"abcdefgh") == ""
    drain(q)
    before = (q.wire, q.head, q.tail, q.pos, q.bytes)
    q.complete()
    q.complete()
    assert (q.wire, q.head, q.tail, q.pos, q.bytes) == before, (
        "a stray completion moved the queue's state")


def case_source_still_matches():
    """The transcription above is only worth anything if the source agrees."""
    with open(SOURCE, encoding="utf-8") as fh:
        src = fh.read()

    required = [
        ("the enqueue entry point", r"private function __enqueueWrite\b"),
        ("the pump", r"private command __pumpWriteQueue\b"),
        ("the completion handler", r"on mqttSocketWriteDone\b"),
        ("the async write helper", r"private function __writeSocketAsync\b"),
        ("the drop path", r"private command __dropWriteQueue\b"),
        ("emptiness tested as head > tail", r"if tHead > tTail then"),
        ("the in-flight size is recorded before the write",
         r'put \(tEnd - tPos \+ 1\) into gMQTTWriteQueues\[pSocketID\]\["inflight"\]'),
        ("the completion advances by the recorded size",
         r'put gMQTTWriteQueues\[pSocketID\]\["inflight"\] into tSent'),
        ("backpressure against the buffer ceiling",
         r"__queuedBytes\(pSocketID\) \+ tTotal > __maxBufferSize\(\)"),
        ("DISCONNECT is written synchronously",
         r"put __writeSocketSync\(tConnID, tPacket\) into tErr"),
        ("the queue is dropped on teardown", r"__dropWriteQueue pConnID"),
    ]
    for what, pattern in required:
        if not re.search(pattern, src):
            raise AssertionError(
                "%s is gone from libMQTTxt.oxtstack (/%s/) - this model no "
                "longer describes the library" % (what, pattern))


CASES = [
    ("packets reach the socket in order", case_fifo_single),
    ("interleaved enqueues keep their order", case_fifo_interleaved),
    ("a packet queued during a write waits its turn", case_enqueue_during_completion),
    ("a mid-flight chunk-size change cannot corrupt", case_chunk_size_changes_mid_flight),
    ("the queue refuses more than its limit", case_backpressure),
    ("a closed socket stops the queue", case_socket_closes_mid_packet),
    ("an empty packet is a no-op", case_empty_packet),
    ("a stray completion changes nothing", case_spurious_completion),
    ("the source still matches this model", case_source_still_matches),
]


def main():
    failures = 0
    for name, fn in CASES:
        try:
            fn()
        except AssertionError as exc:
            print("FAIL %s: %s" % (name, exc))
            failures += 1
        else:
            print("ok   %s" % name)

    if failures:
        print("\ntest-write-queue: %d failure(s)" % failures, file=sys.stderr)
        return 1
    print("\ntest-write-queue: OK (%d properties)" % len(CASES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
