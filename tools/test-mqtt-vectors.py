#!/usr/bin/env python3
"""Protocol vectors for libMQTTxt's framing, checked headlessly.

CI cannot run OXT, but the framing is pure computation, so it can be proved
here: each encoder in libMQTTxt.oxtstack is transliterated line-for-line into
Python below, and the transliteration is checked against byte sequences taken
from MQTT 3.1.1 rather than from this library. The transliterations are then
diffed against the real source, so a change to the library that this file does
not follow is a failure rather than a stale pass.

Run with no arguments; exits non-zero on any mismatch.
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SOURCE = os.path.join(ROOT, "libMQTTxt.oxtstack")

FAILURES = []


def check(name, got, want):
    if got == want:
        print("ok   %s" % name)
    else:
        FAILURES.append(name)
        print("FAIL %s\n       got  %r\n       want %r" % (name, got, want))


# ---------------------------------------------------------------------------
# Transliterations of the library's encoders
# ---------------------------------------------------------------------------

def encode_remaining_length(n):
    """__encodeRemainingLength"""
    if not isinstance(n, int) or n < 0 or n > 268435455:
        return None
    out = bytearray()
    while True:
        byte = n % 128
        n = n // 128
        if n > 0:
            byte |= 0x80
        out.append(byte)
        if n <= 0:
            return bytes(out)


def decode_remaining_length(buf, start_pos):
    """__decodeRemainingLength (1-based start_pos, as in xTalk)"""
    multiplier, value, pos = 1, 0, start_pos
    for _ in range(4):
        if pos > len(buf):
            return None
        byte = buf[pos - 1]
        value += (byte & 0x7F) * multiplier
        multiplier *= 128
        pos += 1
        if (byte & 0x80) == 0:
            return (value, pos - start_pos)
    return None


def encode_string(data):
    """__encodeString - a 16-bit BYTE-count prefix"""
    if len(data) > 65535:
        return None
    return len(data).to_bytes(2, "big") + data


def encode_utf8_string(text):
    """__encodeUTF8String"""
    return encode_string(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# Vectors from MQTT 3.1.1, not from this library
# ---------------------------------------------------------------------------

def test_remaining_length():
    # Table 2.4 of MQTT 3.1.1 gives the byte-count boundaries exactly.
    check("remaining length 0", encode_remaining_length(0), b"\x00")
    check("remaining length 127 (1 byte max)", encode_remaining_length(127), b"\x7f")
    check("remaining length 128 (2 byte min)", encode_remaining_length(128), b"\x80\x01")
    check("remaining length 16383 (2 byte max)", encode_remaining_length(16383), b"\xff\x7f")
    check("remaining length 16384 (3 byte min)", encode_remaining_length(16384), b"\x80\x80\x01")
    check("remaining length 2097151 (3 byte max)", encode_remaining_length(2097151), b"\xff\xff\x7f")
    check("remaining length 2097152 (4 byte min)", encode_remaining_length(2097152), b"\x80\x80\x80\x01")
    check("remaining length 268435455 (4 byte max)",
          encode_remaining_length(268435455), b"\xff\xff\xff\x7f")
    # Past the maximum the varint cannot be expressed; the library must refuse
    # rather than emit a fifth continuation byte no broker can parse.
    check("remaining length 268435456 refused", encode_remaining_length(268435456), None)
    check("negative remaining length refused", encode_remaining_length(-1), None)

    for n in (0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455):
        encoded = encode_remaining_length(n)
        check("round trip %d" % n, decode_remaining_length(encoded, 1),
              (n, len(encoded)))

    # An incomplete varint must report "need more", not a wrong answer
    check("truncated varint reports incomplete", decode_remaining_length(b"\x80", 1), None)
    check("varint past buffer end reports incomplete",
          decode_remaining_length(b"\x00", 5), None)


def test_string_encoding():
    # Section 1.5.3: a UTF-8 encoded string is a 2-byte BYTE count then the bytes
    check("encode 'MQTT'", encode_utf8_string("MQTT"), b"\x00\x04MQTT")
    check("encode empty string", encode_utf8_string(""), b"\x00\x00")

    # The framing bug this library shipped: two U+00E9 are TWO characters and
    # FOUR bytes. A character count would write 0x0002 and desynchronise the
    # broker on the very next packet.
    check("encode 2 chars / 4 bytes", encode_utf8_string("éé"),
          b"\x00\x04\xc3\xa9\xc3\xa9")
    check("encode 1 char / 4 bytes (astral)", encode_utf8_string("\U0001F600"),
          b"\x00\x04\xf0\x9f\x98\x80")

    # 65535 bytes fits; 65536 cannot be expressed in the 2-byte prefix
    check("65535 bytes fits", encode_string(b"x" * 65535)[:2], b"\xff\xff")
    check("65536 bytes refused", encode_string(b"x" * 65536), None)


def test_connect_packet():
    """__buildConnectPacket, for the minimal client the spec illustrates."""
    var_header = len(b"MQTT").to_bytes(2, "big") + b"MQTT" + bytes([4])
    flags = 0x02                                   # clean session
    var_header += bytes([flags]) + (60).to_bytes(2, "big")
    payload = encode_utf8_string("libMQTTxt")
    remaining = len(var_header) + len(payload)
    packet = bytes([0x10]) + encode_remaining_length(remaining) + var_header + payload

    check("CONNECT fixed header byte", packet[0:1], b"\x10")
    check("CONNECT protocol name", packet[2:8], b"\x00\x04MQTT")
    check("CONNECT protocol level 4 (3.1.1)", packet[8:9], b"\x04")
    check("CONNECT clean-session flag", packet[9:10], b"\x02")
    check("CONNECT keep-alive 60", packet[10:12], b"\x00\x3c")
    check("CONNECT declared length matches body",
          decode_remaining_length(packet, 2)[0], len(packet) - 2)


def test_publish_packet():
    """__buildPublishPacket first byte, and the length the header declares."""
    for qos, retain, want in ((0, False, 0x30), (1, False, 0x32), (2, False, 0x34),
                              (0, True, 0x31), (1, True, 0x33)):
        first = 0x30
        if qos == 1:
            first |= 0x02
        elif qos == 2:
            first |= 0x04
        if retain:
            first |= 0x01
        check("PUBLISH first byte QoS %d retain %s" % (qos, retain), first, want)

    # A payload whose bytes and characters differ is the case the library got
    # wrong: the declared Remaining Length must match the bytes actually sent.
    topic = "sensor/é".encode("utf-8")
    payload = "temp=21°C".encode("utf-8")
    var_header = encode_string(topic)
    remaining = len(var_header) + len(payload)
    packet = bytes([0x30]) + encode_remaining_length(remaining) + var_header + payload
    declared, varint_len = decode_remaining_length(packet, 2)
    check("PUBLISH declared length equals real body length",
          declared, len(packet) - 1 - varint_len)
    check("PUBLISH topic prefix is a byte count", var_header[:2],
          len(topic).to_bytes(2, "big"))


def test_ack_packets():
    """Two-byte acknowledgment packets, per sections 3.4 - 3.7."""
    for name, first in (("PUBACK", 0x40), ("PUBREC", 0x50),
                        ("PUBREL", 0x62), ("PUBCOMP", 0x70)):
        packet = bytes([first, 2]) + (1234).to_bytes(2, "big")
        check("%s is 4 bytes" % name, len(packet), 4)
        check("%s packet id round trips" % name,
              int.from_bytes(packet[2:4], "big"), 1234)
    # PUBREL reserves bits 3-0 as 0010; anything else is a protocol violation
    check("PUBREL reserved bits", 0x62 & 0x0F, 0x02)
    check("SUBSCRIBE reserved bits", 0x82 & 0x0F, 0x02)
    check("UNSUBSCRIBE reserved bits", 0xA2 & 0x0F, 0x02)


# ---------------------------------------------------------------------------
# Keep the transliterations honest
# ---------------------------------------------------------------------------

def test_source_still_matches():
    """The vectors above prove the ALGORITHM; this proves it is still the one
    the library uses. Without it, a change to the .oxtstack would leave these
    tests passing against a Python copy of code that no longer exists."""
    with open(SOURCE, encoding="utf-8") as fh:
        src = fh.read()

    expectations = [
        # __encodeRemainingLength: the 268435455 ceiling and the varint loop
        (r"pLength > 268435455", "remaining-length ceiling"),
        (r"put pLength mod 128 into tByte", "remaining-length varint step"),
        (r"put tByte bitOr 0x80 into tByte", "remaining-length continuation bit"),
        # __decodeRemainingLength: 4 bytes maximum, byte-addressed
        (r"repeat 4 times", "remaining-length decode bound"),
        (r"put byteToNum\(byte tPos of pBuffer\) into tByte", "byte-addressed decode"),
        # __encodeString: 16-bit BYTE count
        (r"put the number of bytes of pString into tLen", "byte-count prefix"),
        (r"tLen > 65535", "16-bit prefix ceiling"),
        (r'binaryEncode\("n", tLen\) & pString', "big-endian 16-bit prefix"),
        # Packet type constants
        (r"put __numToByte\(0x10\) into tPacket", "CONNECT type byte"),
        (r"put 0x30 into tFirstByte", "PUBLISH type byte"),
        (r"put __numToByte\(0x82\) into tPacket", "SUBSCRIBE type + reserved bits"),
        (r"put __numToByte\(0xA2\) into tPacket", "UNSUBSCRIBE type + reserved bits"),
        (r"put __numToByte\(0x62\) into tPacket", "PUBREL type + reserved bits"),
        (r'__numToByte\(0xE0\) & __numToByte\(0\)', "DISCONNECT packet"),
        (r'__numToByte\(0xC0\) & __numToByte\(0\)', "PINGREQ packet"),
        (r'binaryEncode\("n", 4\)', "protocol name length"),
        (r"put tVarHeader & __numToByte\(4\) into tVarHeader", "protocol level 4"),
    ]

    for pattern, what in expectations:
        check("source still carries %s" % what,
              bool(re.search(pattern, src)), True)


def main():
    if not os.path.exists(SOURCE):
        print("test-mqtt-vectors: cannot find %s" % SOURCE, file=sys.stderr)
        return 2

    test_remaining_length()
    test_string_encoding()
    test_connect_packet()
    test_publish_packet()
    test_ack_packets()
    test_source_still_matches()

    if FAILURES:
        print("\ntest-mqtt-vectors: %d failure(s)" % len(FAILURES), file=sys.stderr)
        return 1
    print("\ntest-mqtt-vectors: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
