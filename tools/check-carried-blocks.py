#!/usr/bin/env python3
"""check-carried-blocks.py - the UI kit and the boot self-check are ONE block
each, byte-identical everywhere they are carried.

tools/ui-kit.livecodescript and tools/demo-selfcheck.livecodescript are the
masters (vendored from xtalk-suite - see tools/VENDORED.md); each adopting demo
embeds the block between its marker lines verbatim, so every demo stays a single
paste-and-run file while the family keeps one look and one self-check.

This merges what the suite keeps as check-ui-kit-drift.py and
check-demo-selfcheck-drift.py. They are one file here because this repo has one
demo and two blocks: two near-identical 200-line gates over one adopter is the
copy-paste shape both of them exist to prevent.

Four failure modes per block, all fatal:
  - a registered adopter whose embedded block differs from the master
    (drift: the block was patched in place instead of in the master);
  - a file that carries the BEGIN marker but is not registered below
    (adoption is deliberate, so a new adopter is a one-line change HERE in the
    same commit);
  - a registered adopter with no marker (the block was dropped or the file
    moved, and the registry would otherwise rot into a list nobody re-reads);
  - a stack that BUILDS A WINDOW but neither adopts the kit nor carries a
    written exemption. This is what makes "every demo is a kit adopter" a
    property of the tree rather than of one cleanup pass.

And one that belongs only to the self-check, because it is what a copy-paste
rollout actually produces: a demo that carries the block, ships the plumbing,
and reports nothing because it never calls scBegin or never reaches its run
handler.

USAGE
    python3 tools/check-carried-blocks.py            # gate
    python3 tools/check-carried-blocks.py --write    # re-carry from the masters
"""

import glob
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

DEMO = os.path.join("examples", "mqtt-dashboard.livecodescript")

# Each block: master, marker lines, and the adopters that carry it.
BLOCKS = {
    "ui-kit": {
        "master": os.path.join("tools", "ui-kit.livecodescript"),
        "begin": ("-- ==== SUITE UI KIT v2 BEGIN (verbatim copy; master: "
                  "tools/ui-kit.livecodescript; gate: "
                  "tools/check-ui-kit-drift.py) ===="),
        "end": "-- ==== SUITE UI KIT v2 END ====",
        "adopters": [DEMO],
    },
    "demo-selfcheck": {
        "master": os.path.join("tools", "demo-selfcheck.livecodescript"),
        "begin": ("-- ==== DEMO SELF-CHECK v1 BEGIN (verbatim copy; master: "
                  "tools/demo-selfcheck.livecodescript; gate: "
                  "tools/check-demo-selfcheck-drift.py) ===="),
        "end": "-- ==== DEMO SELF-CHECK v1 END ====",
        "adopters": [DEMO],
    },
}

# Window-building stacks that legitimately do NOT carry the kit, each with the
# reason a reader needs. A stale entry (file gone, or it adopted after all)
# fails the gate, so this cannot rot into folklore.
EXEMPT = {}

# The three spellings that mean "this file builds a window".
BUILDS_WINDOW = re.compile(
    r"^\s*(?:uiChrome\b"
    r"|set\s+the\s+(?:width|height)\s+of\s+this\s+stack\b"
    r"|set\s+the\s+rect\s+of\s+this\s+stack\b)", re.M | re.I)

# Files bannered as fully generated are skipped: they are outputs, not sources.
GENERATED = "GENERATED - do not edit"


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def master_block(spec):
    """The master's own block text, markers included."""
    text = read(os.path.join(ROOT, spec["master"]))
    start = text.find(spec["begin"])
    if start < 0:
        return None, "master %s: no BEGIN marker" % spec["master"]
    end = text.find(spec["end"], start)
    if end < 0:
        return None, "master %s: BEGIN marker with no END" % spec["master"]
    return text[start:end + len(spec["end"])], None


def carried_span(text, spec):
    """(start, end) of the carried block in an adopter, or None."""
    start = text.find(spec["begin"])
    if start < 0:
        return None
    end = text.find(spec["end"], start)
    if end < 0:
        return None
    return (start, end + len(spec["end"]))


def scan_sources():
    """Every .livecodescript / .oxtstack in the tree, repo-relative."""
    out = []
    for pattern in ("**/*.livecodescript", "**/*.oxtstack"):
        for p in glob.glob(os.path.join(ROOT, pattern), recursive=True):
            rel = os.path.relpath(p, ROOT)
            if rel.startswith("tools" + os.sep):
                continue          # the masters themselves
            out.append(rel)
    return sorted(set(out))


def main(argv):
    write = "--write" in argv
    problems, rewrote = [], 0
    sources = scan_sources()

    for name, spec in sorted(BLOCKS.items()):
        block, err = master_block(spec)
        if err:
            problems.append(err)
            continue

        registered = set(spec["adopters"])

        # 1 + 3: every registered adopter carries the block, and it matches.
        for rel in sorted(registered):
            path = os.path.join(ROOT, rel)
            if not os.path.exists(path):
                problems.append("%s: registered %s adopter does not exist"
                                % (rel, name))
                continue
            text = read(path)
            span = carried_span(text, spec)
            if span is None:
                problems.append(
                    "%s: registered as a %s adopter but carries no block. "
                    "Carry it, or drop the registration." % (rel, name))
                continue
            have = text[span[0]:span[1]]
            if have == block:
                continue
            if write:
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(text[:span[0]] + block + text[span[1]:])
                rewrote += 1
                print("%s: re-carried %s from the master" % (rel, name))
            else:
                problems.append(
                    "%s: the carried %s block DIFFERS from %s. A look or "
                    "check change is made in the master and re-carried, never "
                    "patched in an adopter. Run with --write."
                    % (rel, name, spec["master"]))

        # 2: nobody carries it without registering.
        for rel in sources:
            if rel in registered:
                continue
            text = read(os.path.join(ROOT, rel))
            if text[:4000].find(GENERATED) >= 0:
                continue
            if spec["begin"] in text:
                problems.append(
                    "%s: carries the %s block but is not registered in "
                    "tools/check-carried-blocks.py. Adoption is deliberate: "
                    "add it in the same commit." % (rel, name))

    # 4: a window-building stack must adopt the kit or be exempt.
    kit = BLOCKS["ui-kit"]
    for rel in sources:
        text = read(os.path.join(ROOT, rel))
        if text[:4000].find(GENERATED) >= 0:
            continue
        if not BUILDS_WINDOW.search(text):
            continue
        if rel in kit["adopters"] or rel in EXEMPT:
            continue
        problems.append(
            "%s: builds a window but neither carries the UI kit nor has a "
            "written exemption in tools/check-carried-blocks.py. One demo "
            "forking the look is how a family loses one." % rel)

    for rel in sorted(EXEMPT):
        if not os.path.exists(os.path.join(ROOT, rel)):
            problems.append("%s: exempted but does not exist - a stale "
                            "exemption is folklore." % rel)
        elif rel in kit["adopters"]:
            problems.append("%s: both exempted and registered as an adopter."
                            % rel)

    # 5: the self-check must actually RUN. A demo that carries the plumbing and
    # never calls scBegin reports nothing, and reads exactly like a green one.
    sc = BLOCKS["demo-selfcheck"]
    for rel in sorted(set(sc["adopters"])):
        path = os.path.join(ROOT, rel)
        if not os.path.exists(path):
            continue
        text = read(path)
        span = carried_span(text, sc)
        own = text[:span[0]] + text[span[1]:] if span else text

        if not re.search(r"^\s*scBegin\b", own, re.M):
            problems.append(
                "%s: carries the self-check block but never calls scBegin, so "
                "it reports nothing - which reads exactly like a green run."
                % rel)
        if not re.search(r"^\s*scArmProbe\b", own, re.M):
            problems.append(
                "%s: never calls scArmProbe, so the delayed-write check never "
                "runs and the block never prints its count line." % rel)

        runner = re.search(r"^command\s+(\w*ScRun)\b", own, re.M)
        if not runner:
            problems.append("%s: no `command <pfx>ScRun` handler." % rel)
        elif not re.search(r"^\s*%s\b" % runner.group(1), own, re.M | re.I):
            problems.append(
                "%s: defines %s but nothing calls it - the block is shipped "
                "and unreached." % (rel, runner.group(1)))

    for p in problems:
        print(p)
    if problems:
        print("\ncheck-carried-blocks: %d problem(s)" % len(problems),
              file=sys.stderr)
        return 1

    if write:
        print("check-carried-blocks: OK (%d block(s) re-carried)" % rewrote)
    else:
        print("check-carried-blocks: OK (%d block(s), %d adopter(s), %d "
              "source(s) scanned)"
              % (len(BLOCKS), len({a for b in BLOCKS.values()
                                   for a in b["adopters"]}), len(sources)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
