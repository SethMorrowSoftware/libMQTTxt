#!/usr/bin/env python3
"""Mutation tests for the demo gates.

Same argument as tools/test-check-libmqttxt.py, applied to the four gates that
guard the demo: a gate that has gone blind reports OK, and OK is what a blind
gate and a clean tree both look like from the outside. Each rule below is
proved by editing a REAL copy of the tree to reintroduce the defect it exists
for, and expecting the gate to fire on it.

The tree is copied rather than mutated in place, so a failing run leaves the
working tree untouched.

Run with no arguments; exits non-zero if any rule failed to discriminate.
"""

import os
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

DEMO = os.path.join("examples", "mqtt-dashboard.livecodescript")
LIB = "libMQTTxt.oxtstack"

GATES = ("check-carried-blocks.py", "check-demo-control-lists.py",
         "check-demo-layout.py", "check-timer-stack-pin.py",
         "sync-demo-embeds.py")

TOOL_FILES = GATES + ("ui-kit.livecodescript", "demo-selfcheck.livecodescript")


def make_tree(tmp, name):
    tree = os.path.join(tmp, name)
    os.makedirs(os.path.join(tree, "tools"))
    os.makedirs(os.path.join(tree, "examples"))
    for f in TOOL_FILES:
        shutil.copy(os.path.join(ROOT, "tools", f), os.path.join(tree, "tools", f))
    shutil.copy(os.path.join(ROOT, LIB), os.path.join(tree, LIB))
    shutil.copy(os.path.join(ROOT, DEMO), os.path.join(tree, DEMO))
    return tree


def run(tree, gate, *args):
    proc = subprocess.run(
        [sys.executable, os.path.join(tree, "tools", gate)] + list(args),
        capture_output=True, text=True, cwd=tree)
    return proc.returncode, proc.stdout + proc.stderr


def edit(tree, rel, old, new, count=1):
    path = os.path.join(tree, rel)
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    if text.count(old) < count:
        raise AssertionError(
            "fixture no longer matches %s; expected %d of %r, found %d"
            % (rel, count, old[:70], text.count(old)))
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text.replace(old, new, count))


# (name, gate, args, mutation, substring the complaint must contain)
CASES = [
    ("drifted UI kit block", "check-carried-blocks.py", (),
     lambda t: edit(t, DEMO, 'constant kUiAccent = "44,90,160"',
                    'constant kUiAccent = "200,0,0"'),
     "DIFFERS from"),

    ("drifted self-check block", "check-carried-blocks.py", (),
     lambda t: edit(t, DEMO, 'command scAssert pName, pOk',
                    'command scAssert pName, pOk, pUnused'),
     "DIFFERS from"),

    ("a demo that never runs its self-check", "check-carried-blocks.py", (),
     lambda t: edit(t, DEMO, '   scBegin "mdLog"', '   -- scBegin removed'),
     "never calls scBegin"),

    ("a self-check that never arms the probe", "check-carried-blocks.py", (),
     lambda t: edit(t, DEMO, "   scArmProbe\nend mdScRun",
                    "   -- probe removed\nend mdScRun"),
     "never calls scArmProbe"),

    ("a phantom name in the control list", "check-demo-control-lists.py", (),
     lambda t: edit(t, DEMO, 'constant kMdScControls = "mdAuto,',
                    'constant kMdScControls = "mdGhost,mdAuto,'),
     "does not build or reference"),

    ("a control the list omits", "check-demo-control-lists.py", (),
     lambda t: edit(t, DEMO, 'constant kMdScControls = "mdAuto,',
                    'constant kMdScControls = "'),
     "omits"),

    ("a control past the bottom edge", "check-demo-layout.py", (),
     lambda t: edit(t, DEMO, 'uiArea "mdLog", "522,474,968,544"',
                    'uiArea "mdLog", "522,474,968,900"'),
     "falls outside"),

    ("a control straddling its panel", "check-demo-layout.py", (),
     lambda t: edit(t, DEMO, 'uiInput "mdHost", "32,128,340,152"',
                    'uiInput "mdHost", "32,128,540,152"'),
     "straddles the edge of panel"),

    ("two controls on top of each other", "check-demo-layout.py", (),
     lambda t: edit(t, DEMO, 'uiInput "mdPort", "352,128,478,152"',
                    'uiInput "mdPort", "32,128,340,152"'),
     "overlaps"),

    ("a window past the 720p budget", "check-demo-layout.py", (),
     lambda t: edit(t, DEMO, 'uiChrome "libMQTTxt - MQTT Dashboard", 1000, 620, 34',
                    'uiChrome "libMQTTxt - MQTT Dashboard", 1000, 900, 34'),
     "720p budget"),

    ("one control built at two different rects", "check-demo-layout.py", (),
     lambda t: edit(t, DEMO, '   mdSetPill "offline", "bad"',
                    '   uiPill "mdConnPill", "offline", "386,78,470,98", "bad"'),
     "Give the rect ONE owner"),

    ("an unpinned delayed handler", "check-timer-stack-pin.py", (),
     lambda t: edit(t, DEMO,
                    "   set the defaultStack to the short name of this stack\n\n"
                    "   local tStats, tLine, tErr",
                    "   local tStats, tLine, tErr"),
     "armed by `send ... to me in`"),

    ("a stale embedded library", "sync-demo-embeds.py", ("--check",),
     lambda t: edit(t, LIB, "function mqttTestLibrary",
                    "function mqttTestLibraryRenamed"),
     "STALE"),

    ("a collision between demo and library", "sync-demo-embeds.py", (),
     lambda t: edit(t, DEMO, "command mdBuild", "command mqttPublish"),
     "compile"),
]


def main():
    failures = []
    with tempfile.TemporaryDirectory() as tmp:
        # THE CONTROL. Every case below "fires" for the wrong reason if the
        # gates do not first pass on an unmutated tree.
        clean = make_tree(tmp, "clean")
        for gate in GATES:
            args = ("--check",) if gate == "sync-demo-embeds.py" else ()
            code, out = run(clean, gate, *args)
            if code != 0:
                print("CONTROL FAILED: %s does not pass on a clean tree\n%s"
                      % (gate, out))
                return 1
        print("ok   control: all %d gates pass on the unmutated tree" % len(GATES))

        for i, (name, gate, args, mutation, expected) in enumerate(CASES):
            tree = make_tree(tmp, "case%02d" % i)
            try:
                mutation(tree)
            except AssertionError as exc:
                failures.append("%s: %s" % (name, exc))
                print("FAIL %s: %s" % (name, exc))
                continue

            code, out = run(tree, gate, *args)
            if code == 0:
                failures.append("%s: %s did NOT fire" % (name, gate))
                print("FAIL %s: %s did not fire" % (name, gate))
            elif expected not in out:
                failures.append("%s: fired but not for this reason (wanted %r)"
                                % (name, expected))
                print("FAIL %s: fired on something else\n%s" % (name, out))
            else:
                print("ok   %s" % name)

    if failures:
        print("\ntest-demo-gates: %d rule(s) failed to discriminate"
              % len(failures), file=sys.stderr)
        return 1
    print("\ntest-demo-gates: OK (%d rules proved discriminating)" % len(CASES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
