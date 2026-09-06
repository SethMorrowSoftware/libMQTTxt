#!/usr/bin/env python3
"""Static gate for libMQTTxt.oxtstack.

OXT has no headless way to compile or run a `.livecodescript` / `.oxtstack`, so
every rule here is a stand-in for a compiler that cannot be run in CI. Each one
is a defect that actually shipped in this library, written down so it cannot
ship twice.

Run with no arguments from anywhere in the repo; exits non-zero on any finding.
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TARGET = os.path.join(ROOT, "libMQTTxt.oxtstack")

# ---------------------------------------------------------------------------
# Source scanning
# ---------------------------------------------------------------------------

HANDLER_RE = re.compile(
    r"^\s*(private\s+)?(on|command|function)\s+([A-Za-z_][A-Za-z0-9_]*)\s*(.*)$"
)

# `end if`, `end try`, `end repeat` and `end switch` close a BLOCK, not a
# handler. Matching them as handler ends is the defect this gate's own first run
# reported against itself: every scan below stopped at the first `end if` and
# read the rest of the handler as though it were top level.
BLOCK_ENDS = {"if", "try", "repeat", "switch"}
_END_RE = re.compile(r"^\s*end\s+([A-Za-z_][A-Za-z0-9_]*)\s*$")


def handler_end(stripped):
    """True when this line ends a HANDLER (not a block)."""
    m = _END_RE.match(stripped)
    return bool(m) and m.group(1) not in BLOCK_ENDS


def strip_noise(line):
    """Blank out comments and string literals, tracking string state.

    The usual noise-stripper blanks literals only; here the literals matter as
    little as the comments, but a `--` INSIDE a literal must not start one.
    """
    out = []
    in_str = False
    i = 0
    while i < len(line):
        c = line[i]
        if in_str:
            out.append(" ")
            if c == '"':
                in_str = False
        elif c == '"':
            in_str = True
            out.append(" ")
        elif c == "-" and line[i : i + 2] == "--":
            out.append(" " * (len(line) - i))
            break
        else:
            out.append(c)
        i += 1
    return "".join(out)


def strip_comment_only(line):
    """Blank the comment, KEEP the string literals.

    strip_noise blanks literals too, and a scan that needs a literal's CONTENT
    must not use it: `field "y"` becomes `field    ` there, so a rule looking
    for a control reference reads a clean line. This gate's own first run of
    check_no_control_references passed its mutation test's planted bug for
    exactly that reason - the third time in this family that literal-blanking
    silently changed an answer.
    """
    out, in_str, i = [], False, 0
    while i < len(line):
        c = line[i]
        if c == '"':
            in_str = not in_str
        elif not in_str and line[i:i + 2] == "--":
            break
        out.append(c)
        i += 1
    return "".join(out)


def load(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read().split("\n")


def handlers(lines):
    """Map handler name -> (kind, parameter count, line number)."""
    found = {}
    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        m = HANDLER_RE.match(stripped)
        if not m:
            continue
        kind, name, params = m.group(2), m.group(3), m.group(4).strip()
        count = len([p for p in params.split(",") if p.strip()]) if params else 0
        found[name] = (kind, count, n)
    return found


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_calls(lines, defined, problems):
    """Every private helper called must exist.

    A misspelled or renamed `__helper` does not fail to compile - xTalk reads an
    unknown bare word as a message send that finds no handler at RUNTIME, on
    whatever path happens to reach it first.
    """
    known = set(defined)
    call_re = re.compile(r"\b(__[A-Za-z0-9_]+)\b")
    for n, raw in enumerate(lines, 1):
        for name in call_re.findall(strip_noise(raw)):
            if name.startswith("__") and name not in known:
                problems.append(
                    "%d: calls `%s`, which no handler in this file defines" % (n, name)
                )


def check_binary_semantics(lines, problems):
    """Binary framing must count BYTES, not characters.

    `char`, `charToNum` and `the length of` count characters; a UTF-8 payload
    makes them disagree with the byte count the protocol declares, and every
    packet after the first multi-byte one is misframed. Handlers that only
    touch text (filenames, log lines) are exempt by name.
    """
    text_only = {"__sanitizeFilename", "__scheduleMessage", "mqttSelfTest",
                 "__decodeRemainingLength"}
    current = None
    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        m = HANDLER_RE.match(stripped)
        if m:
            current = m.group(3)
            continue
        if handler_end(stripped):
            current = None
            continue
        if current in text_only:
            continue
        for token, why in (
            (r"\bcharToNum\s*\(", "charToNum"),
            (r"\bthe length of\b", "the length of"),
            (r"\blength\s*\(", "length()"),
        ):
            if re.search(token, stripped):
                problems.append(
                    "%d: `%s` in `%s` counts CHARACTERS; binary framing needs "
                    "`the number of bytes of` / `byteToNum`" % (n, why, current)
                )


def check_engine_socket_messages(defined, problems):
    """The engine's three socket messages must be declared, and split.

    socketClosed / socketError / socketTimeout are the ONLY names the engine
    sends when a socket drops, fails or idles. This library once declared
    `mqttSocketClosed` instead and nothing ever sent it, so a dropped
    connection fired no callback and auto-reconnect was unreachable code.

    They are also required to be thin wrappers over same-named functions, so an
    app that runs its own sockets can drop the wrappers and call the functions -
    two scripts cannot define one of these names in one script.
    """
    for msg in ("socketClosed", "socketError", "socketTimeout"):
        if msg not in defined:
            problems.append(
                "the engine message `%s` is not declared; a dropped socket "
                "will go unnoticed" % msg
            )
            continue
        if defined[msg][0] != "on":
            problems.append("`%s` must be an `on` handler" % msg)

        logic = "mqttS" + msg[1:]
        if logic not in defined:
            problems.append(
                "`%s` has no `%s` function to dispatch to; keep the logic "
                "behind a name of its own so an embedder can drop the wrapper"
                % (msg, logic)
            )
        elif defined[logic][0] != "function":
            problems.append("`%s` must be a function returning ours/not-ours" % logic)


def check_engine_messages_pass(lines, problems):
    """A socket message that is not ours must be PASSED, never swallowed.

    Eating another library's socketClosed is a silent hang for that library, and
    silent is the worst failure this project has.
    """
    for msg in ("socketClosed", "socketError", "socketTimeout"):
        body, inside = [], False
        for raw in lines:
            stripped = strip_noise(raw)
            m = HANDLER_RE.match(stripped)
            if m and m.group(3) == msg:
                inside = True
                continue
            if inside and handler_end(stripped):
                break
            if inside:
                body.append(stripped)
        if body and not any(re.search(r"\bpass\s+%s\b" % msg, b) for b in body):
            problems.append(
                "`on %s` never does `pass %s`; a message belonging to another "
                "socket library would be swallowed" % (msg, msg)
            )


def check_timer_handlers_take_a_token(defined, problems):
    """Timer handlers must be routed, not broadcast.

    Both timer handlers once took no argument and scanned EVERY connection for a
    marker equal to its own key. With two connections open, whichever timer
    fired first serviced BOTH - so a connection was pinged on its neighbour's
    schedule and dropped by the broker for being idle.
    """
    for name in ("__executeKeepAliveTimer", "__executeReconnectTimer"):
        if name not in defined:
            problems.append("timer handler `%s` is missing" % name)
        elif defined[name][1] < 1:
            problems.append(
                "`%s` takes no parameter; a timer must name the ONE connection "
                "it belongs to, not scan for it" % name
            )
        elif defined[name][0] != "on":
            problems.append(
                "`%s` must be an `on` handler to receive a sent message" % name
            )


def check_ascii(path, problems):
    """OXT source in this family is pure ASCII, comments included."""
    with open(path, "rb") as fh:
        data = fh.read()
    line = 1
    for byte in data:
        if byte == 0x0A:
            line += 1
        elif byte > 127:
            problems.append("%d: non-ASCII byte 0x%02X; OXT source must be ASCII"
                            % (line, byte))
            break


def check_bad_operators(lines, problems):
    """`does not contain` / `does not begin with` are not xTalk.

    The parser errors on `does`, and it takes the WHOLE script down, not just
    the handler - which is how two of these made every public handler in this
    library unreachable.
    """
    bad = re.compile(r"\bdoes\s+not\s+(contain|begin\s+with|end\s+with)\b")
    for n, raw in enumerate(lines, 1):
        if bad.search(strip_noise(raw)):
            problems.append(
                "%d: `does not ...` is not an xTalk operator; the parser errors "
                "on `does` and the whole script fails to compile" % n
            )


def check_catch_variables_declared(lines, problems):
    """Every `catch X` must have X declared in the same handler.

    On strict OXT an undeclared catch variable throws a SECOND error when the
    catch fires, which masks the failure you were trying to report.
    """
    current, declared, caught = None, set(), []
    decl_re = re.compile(r"^\s*(?:local|global)\s+(.*)$")
    catch_re = re.compile(r"^\s*catch\s+([A-Za-z_][A-Za-z0-9_]*)\s*$")

    def flush():
        for name, line in caught:
            if name not in declared:
                problems.append(
                    "%d: catch variable `%s` in `%s` is never declared"
                    % (line, name, current)
                )

    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        m = HANDLER_RE.match(stripped)
        if m:
            if current:
                flush()
            current = m.group(3)
            params = m.group(4).strip()
            declared = {p.strip() for p in params.split(",") if p.strip()}
            caught = []
            continue
        if handler_end(stripped):
            if current:
                flush()
            current, declared, caught = None, set(), []
            continue
        if current is None:
            continue
        d = decl_re.match(stripped)
        if d:
            declared.update(x.strip() for x in d.group(1).split(",") if x.strip())
        c = catch_re.match(stripped)
        if c:
            caught.append((c.group(1), n))
    if current:
        flush()


def check_socket_writes_are_checked(lines, problems):
    """Every socket write goes through __writeSocket.

    A failed socket write does not always throw - LiveCode sets `the result` -
    so a bare `write ... to socket` inside a try/catch reports success on a
    write that did not happen. All twelve of this library's writes were shaped
    that way, and it was found on a real broker: a 200 KB PUBLISH left the
    broker holding a fixed header promising bytes that never arrived, after
    which it read everything sent afterwards as that packet's missing tail. It
    stopped acknowledging, stopped echoing and answered no PINGREQ, while the
    client saw clean writes and a socket the engine still called open.

    The same lesson the persistent store already carries for file I/O, one
    layer down. __writeSocket checks both the throw and `the result`.
    """
    current = None
    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        m = HANDLER_RE.match(stripped)
        if m:
            current = m.group(3)
            continue
        if handler_end(stripped):
            current = None
            continue
        # The two legitimate bare writes: the synchronous helper and the
        # asynchronous one. Both check the throw AND `the result`; everything
        # else routes through __writeSocket, which picks between them.
        # Exempted by NAME, so the exemption cannot quietly widen.
        if current in ("__writeSocketSync", "__writeSocketAsync"):
            continue
        if re.search(r"^\s*write\b.*\bto\s+socket\b", stripped):
            problems.append(
                "%d: a bare `write ... to socket`. A failed write sets `the "
                "result` rather than throwing, so this reports success on a "
                "write that did not happen - route it through __writeSocket."
                % n)


def check_no_control_references(lines, problems):
    """The library is HEADLESS: it names no field, button, graphic or image.

    This is not a style rule, it is what makes another gate sound.
    tools/check-timer-stack-pin.py finds delayed handlers by matching
    `send "name" to me in` against raw text - and this library arms its timers
    through `send pHandler to me in`, where the handler name is a VARIABLE. So
    the library's own timer chains are invisible to that gate.

    That blind spot is harmless only while there is nothing to pin: the hazard
    it exists for is an unqualified control reference inside a delayed handler
    (engine note 5.3). A library that touches no control cannot have one. This
    rule holds that precondition, so the day somebody adds `put x into field
    "y"` here, it fails HERE rather than becoming a silent gap over there.

    Callbacks are how this library reaches a UI, and they go out through
    `dispatch` to the application's own handlers - which is the only shape that
    works for a library whose caller owns the window.
    """
    ref = re.compile(r"\b(field|button|graphic|image|scrollbar|player)\s+"
                     r'("[^"]*"|[A-Za-z_]\w*)')
    for n, raw in enumerate(lines, 1):
        m = ref.search(strip_comment_only(raw))
        if m:
            problems.append(
                "%d: names a control (`%s`). This library is headless - it "
                "reaches a UI only through dispatch to the caller's handlers - "
                "and check-timer-stack-pin.py cannot see its variable-named "
                "`send`, so a control reference here would be an unpinned "
                "delayed write no gate is watching." % (n, m.group(0).strip()))


def check_reentrant_waits(lines, problems):
    """`wait ... with messages` yields the engine, and only one place may.

    A yield lets application code run in the middle of a library handler. That
    is what makes the chunked write work - the pending socket read gets its
    turn, our receive buffer drains, and the broker resumes draining us - and it
    is also how a second packet can be written into the middle of the first.
    __writeSocket takes a per-socket lock across its yields and refuses a
    re-entrant write; nothing else in this library is written to survive being
    re-entered.

    So the rule is not "no yields", it is "the yields are in the one handler
    whose invariants account for them". Exempted by NAME, so the exemption
    cannot quietly widen to a handler that has not thought about it.
    """
    current = None
    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        m = HANDLER_RE.match(stripped)
        if m:
            current = m.group(3)
            continue
        if handler_end(stripped):
            current = None
            continue
        if not re.search(r"\bwait\b.*\bwith\s+messages\b", stripped):
            continue
        if current == "__writeSocketSync":
            continue
        problems.append(
            "%d: `wait ... with messages` in `%s`. A yield runs application "
            "code inside this handler, which can re-enter the library; only "
            "__writeSocket is written to survive that (it holds a per-socket "
            "lock across its yields)." % (n, current or "<script level>"))


def check_async_writes_are_queued(lines, problems):
    """An asynchronous write may only be started by the queue's pump.

    `write ... with message` hands bytes to the engine and returns immediately,
    so two of them in flight on one socket interleave on the wire. A PINGREQ
    started while a megabyte of PUBLISH is half-written lands INSIDE that
    publish and desynchronises the stream for good - the same corruption a
    stalled synchronous write used to cause, arrived at from the other side.

    The queue is what makes that impossible: one chunk in flight per socket,
    strictly FIFO, the next started only when the engine reports the last one
    done. That guarantee holds only while __pumpWriteQueue is the sole caller
    of __writeSocketAsync, which is what this rule enforces.
    """
    current = None
    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        m = HANDLER_RE.match(stripped)
        if m:
            current = m.group(3)
            continue
        if handler_end(stripped):
            current = None
            continue
        if not re.search(r"\b__writeSocketAsync\s*\(", stripped):
            continue
        if current in ("__pumpWriteQueue", "__writeSocketAsync"):
            continue
        problems.append(
            "%d: `__writeSocketAsync` called from `%s`. Only __pumpWriteQueue "
            "may start an asynchronous write - a second one in flight on the "
            "same socket interleaves with the first and corrupts the stream. "
            "Queue the packet with __enqueueWrite instead."
            % (n, current or "<script level>"))


def check_per_byte_reads(lines, problems):
    """`read ... for 1 ...` costs one engine message dispatch per byte.

    The no-quantifier form streams whatever has arrived; the suite's onionxt
    confirmed that on a real engine. `for 1` turned a 1 MB payload into a
    million round trips through the message queue.
    """
    for n, raw in enumerate(lines, 1):
        stripped = strip_noise(raw)
        if re.search(r"\bread\s+from\s+socket\b.*\bfor\s+1\b", stripped):
            problems.append(
                "%d: `read from socket ... for 1` dispatches one message PER "
                "BYTE; drop the quantifier to stream what has arrived" % n
            )


def main():
    if not os.path.exists(TARGET):
        print("check-libmqttxt: cannot find %s" % TARGET, file=sys.stderr)
        return 2

    lines = load(TARGET)
    defined = handlers(lines)
    problems = []

    check_ascii(TARGET, problems)
    check_bad_operators(lines, problems)
    check_catch_variables_declared(lines, problems)
    check_calls(lines, defined, problems)
    check_binary_semantics(lines, problems)
    check_engine_socket_messages(defined, problems)
    check_engine_messages_pass(lines, problems)
    check_timer_handlers_take_a_token(defined, problems)
    check_per_byte_reads(lines, problems)
    check_no_control_references(lines, problems)
    check_socket_writes_are_checked(lines, problems)
    check_reentrant_waits(lines, problems)
    check_async_writes_are_queued(lines, problems)

    if problems:
        for p in problems:
            print("libMQTTxt.oxtstack:%s" % p)
        print("\ncheck-libmqttxt: %d problem(s)" % len(problems), file=sys.stderr)
        return 1

    print("check-libmqttxt: OK (%d handlers checked)" % len(defined))
    return 0


if __name__ == "__main__":
    sys.exit(main())
