#!/usr/bin/env python3
"""Mutation tests for tools/check-libmqttxt.py.

A gate that has gone blind reports OK, and OK is exactly what a blind gate and a
clean tree look like from the outside. So each rule is proved here by editing a
REAL copy of the library to reintroduce the defect that rule exists for, and
expecting the gate to fire on it - exercised the same way CI runs it.

Run with no arguments; exits non-zero if any rule failed to discriminate.
"""

import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SOURCE = os.path.join(ROOT, "libMQTTxt.oxtstack")
GATE = os.path.join(HERE, "check-libmqttxt.py")


def run_gate(tree):
    proc = subprocess.run(
        [sys.executable, os.path.join(tree, "tools", "check-libmqttxt.py")],
        capture_output=True, text=True,
    )
    return proc.returncode, proc.stdout + proc.stderr


def mutate(text, old, new, occurrences=1):
    if text.count(old) < occurrences:
        raise AssertionError(
            "fixture no longer matches the source; expected %d of %r, found %d"
            % (occurrences, old[:70], text.count(old))
        )
    return text.replace(old, new, occurrences)


# Each case: (name, mutation, substring the gate's complaint must contain)
CASES = [
    (
        "does-not-contain is not xTalk",
        lambda s: mutate(s, "if not (tStackList contains \"libMQTTxt\") then",
                         "if tStackList does not contain \"libMQTTxt\" then"),
        "not an xTalk operator",
    ),
    (
        "non-ASCII source",
        lambda s: mutate(s, "All tests PASSED", "All tests PASSED ✓"),
        "non-ASCII",
    ),
    (
        "undeclared catch variable",
        lambda s: mutate(s,
                         "   local tError\n   try\n"
                         '      read from socket pSocketID with message "mqttSocketDataAvailable"',
                         "   try\n"
                         '      read from socket pSocketID with message "mqttSocketDataAvailable"'),
        "is never declared",
    ),
    (
        "call to a handler that does not exist",
        lambda s: mutate(s, "__mqttDebug \"PINGRESP received\"",
                         "__mqttDebugg \"PINGRESP received\""),
        "which no handler in this file defines",
    ),
    (
        "character counting in binary framing",
        lambda s: mutate(s, "   put the number of bytes of pString into tLen",
                         "   put the length of pString into tLen"),
        "counts CHARACTERS",
    ),
    (
        "engine socket message not declared",
        lambda s: mutate(s, "on socketClosed pSocketID", "on mqttSocketWasClosed pSocketID"),
        "is not declared",
    ),
    (
        "engine socket message swallowed instead of passed",
        lambda s: mutate(s, "   pass socketTimeout\n", "\n"),
        "would be swallowed",
    ),
    (
        "timer handler broadcasts instead of routing",
        lambda s: mutate(s, "on __executeKeepAliveTimer pToken",
                         "on __executeKeepAliveTimer"),
        "takes no parameter",
    ),
    (
        "per-byte socket read",
        lambda s: mutate(s, "read from socket pSocketID with message",
                         "read from socket pSocketID for 1 with message"),
        "one message PER BYTE",
    ),
    (
        "an unchecked socket write",
        lambda s: mutate(s, "      put __writeSocket(pConnID, tPacket) into tError\n"
                            "      if tError is empty then\n"
                            '         __mqttDebug "PUBREC sent for packet" && pPacketID',
                         "      write tPacket to socket pConnID\n"
                            "      if tError is empty then\n"
                            '         __mqttDebug "PUBREC sent for packet" && pPacketID'),
        "bare `write ... to socket`",
    ),
    (
        "a control reference in the headless library",
        lambda s: mutate(s, '   __mqttDebug "PINGRESP received"',
                         '   put "x" into field "y"'),
        "This library is headless",
    ),
    (
        # The realistic version of this mistake: somebody makes an existing
        # wait "keep the UI responsive" and silently makes the reconnect path
        # re-entrant.
        "a re-entrant yield outside __writeSocketSync",
        lambda s: mutate(s, "         wait 100 milliseconds",
                         "         wait 100 milliseconds with messages"),
        "can re-enter the library",
    ),
    (
        # The realistic version of THIS one: a PINGREQ "does not need to wait
        # in a queue behind a big publish", which is exactly backwards.
        "an asynchronous write that bypasses the queue",
        lambda s: mutate(s, "      return __enqueueWrite(pSocketID, pPacket)",
                         "      return __writeSocketAsync(pSocketID, pPacket)"),
        "Only __pumpWriteQueue may start an asynchronous write",
    ),
]


def main():
    failures = []

    with tempfile.TemporaryDirectory() as tmp:
        clean = os.path.join(tmp, "clean")
        os.makedirs(os.path.join(clean, "tools"))
        shutil.copy(SOURCE, os.path.join(clean, "libMQTTxt.oxtstack"))
        shutil.copy(GATE, os.path.join(clean, "tools", "check-libmqttxt.py"))

        # The control: the gate must PASS on an unmutated tree, or every case
        # below "fires" for the wrong reason.
        code, out = run_gate(clean)
        if code != 0:
            print("CONTROL FAILED: the gate does not pass on a clean tree")
            print(out)
            return 1
        print("ok   control: gate passes on the unmutated library")

        with open(SOURCE, encoding="utf-8") as fh:
            original = fh.read()

        for name, mutation, expected in CASES:
            tree = os.path.join(tmp, name.replace(" ", "_"))
            os.makedirs(os.path.join(tree, "tools"))
            shutil.copy(GATE, os.path.join(tree, "tools", "check-libmqttxt.py"))
            try:
                broken = mutation(original)
            except AssertionError as exc:
                failures.append("%s: %s" % (name, exc))
                print("FAIL %s: %s" % (name, exc))
                continue

            with open(os.path.join(tree, "libMQTTxt.oxtstack"), "w",
                      encoding="utf-8") as fh:
                fh.write(broken)

            code, out = run_gate(tree)
            if code == 0:
                failures.append("%s: gate did NOT fire" % name)
                print("FAIL %s: gate did not fire" % name)
            elif expected not in out:
                failures.append(
                    "%s: gate fired but not for this reason (wanted %r)"
                    % (name, expected)
                )
                print("FAIL %s: fired on something else\n%s" % (name, out))
            else:
                print("ok   %s" % name)

    if failures:
        print("\ntest-check-libmqttxt: %d rule(s) failed to discriminate"
              % len(failures), file=sys.stderr)
        return 1

    print("\ntest-check-libmqttxt: OK (%d rules proved discriminating)" % len(CASES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
