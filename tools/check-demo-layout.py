#!/usr/bin/env python3
"""check-demo-layout.py - the demo's window fits a 720p screen, and every
control fits the window.

TWO GATES IN ONE, because the family's own experience is that the first
without the second is a half-measure.

THE BUDGET. The suite's demos are run on whatever hardware is in the room. A
1280x720 display keeps roughly 640px of height once the OS taskbar (~48) and
the title bar (~32) are gone, and a shade under 1280 of width once borders are
counted:

    width  <= 1200
    height <= 640

xtalk-suite's tools/check-stack-size.py holds exactly this, and it exists
because a stack shipped at 780x1010 with its last section BELOW the bottom edge
of precisely the screens it was meant to be demonstrated on.

THE PART THAT GATE CANNOT SEE, and this one does. check-stack-size.py reads ONE
number per axis and stops - it cannot tell whether a field or a button sits
inside the window it just measured, so a control pushed past the bottom edge
passes it in silence: the same failure as the 780x1010 stack, one level down.
The suite closed that for its one dense demo with a member-local
check-table-layout.py. This demo's 49 hand-written rects are dense in the same
way, and there is no engine here to look at them, so the equivalent gate is
carried from the start rather than after the first clipped control.

WHAT IT CHECKS
    1. the window fits the 720p budget;
    2. every rect is well-formed (left < right, top < bottom);
    3. every control lies inside the window;
    4. every control placed on a panel lies inside that panel;
    5. no two non-background controls overlap.

WHAT IT CANNOT CHECK, and says so rather than implying otherwise: a control's
rect is where the builder is ASKED to put it, and several kit builders resize
by MEASURED text height afterwards (uiWrap, uiCap, uiSection, uiPill, the
uiChrome title). Those can only grow a control downward from its top, so this
gate reads the requested rect as a floor, not as the final geometry. A caption
whose text wraps to three lines on one platform's metrics still needs eyes.

USAGE
    python3 tools/check-demo-layout.py
"""

import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

KIT = os.path.join(ROOT, "tools", "ui-kit.livecodescript")

DEMOS = [os.path.join("examples", "mqtt-dashboard.livecodescript")]

MAX_WIDTH = 1200
MAX_HEIGHT = 640

SPANS = [(">>> BEGIN EMBEDDED LIBRARIES", "<<< END EMBEDDED LIBRARIES"),
         ("==== SUITE UI KIT v2 BEGIN", "==== SUITE UI KIT v2 END"),
         ("==== DEMO SELF-CHECK v1 BEGIN", "==== DEMO SELF-CHECK v1 END")]

# Panels are BACKGROUNDS: the kit says to create them first so they sit behind
# the controls placed on them, so they are expected to contain other controls
# and are excluded from the overlap check.
BACKGROUND_BUILDERS = {"uiPanel", "uiGfx"}

# Which builder argument is the rect. Every pName-first builder takes it in a
# fixed position, and the position is read from the master rather than assumed.
RECT_ARG = {
    "uiLabel": 3, "uiWrap": 3, "uiCap": 3, "uiSection": 3,
    "uiInput": 2, "uiArea": 2, "uiTable": 2, "uiButton": 3,
    "uiCheckbox": 3, "uiGfx": 2, "uiPanel": 2, "uiPill": 3,
}

# pName-first builders that take NO rect from the caller. Listed rather than
# left out, so the "the kit gained a builder" check below stays meaningful: an
# unlisted builder is one whose controls this gate would silently skip.
NO_RECT = {
    # uiContextMenu takes an item list; it places its own hidden button at a
    # small fixed rect inside the window, precisely so the size gate counts it.
    "uiContextMenu",
}


def strip_comment(line):
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


def demo_source(path):
    """The demo's own code, comments cut and carried spans removed, with
    continuation lines joined so a wrapped builder call reads as one."""
    with open(path, encoding="utf-8", errors="replace") as fh:
        lines = fh.read().split("\n")
    keep, skip = [], None
    for l in lines:
        if skip is None:
            hit = next((s for s in SPANS if s[0] in l), None)
            if hit:
                skip = hit[1]
                continue
            keep.append(strip_comment(l))
        elif skip in l:
            skip = None
    return re.sub(r"\\\s*\n\s*", " ", "\n".join(keep))


def known_builders():
    """Sanity: every builder this gate knows a rect position for must still
    exist in the kit, and every pName-first kit builder must be known here.
    A kit that gains a builder silently drops its controls out of this gate."""
    with open(KIT, encoding="utf-8") as fh:
        text = fh.read()
    kit = {m.group(1)
           for m in re.finditer(r"^command\s+(ui\w+)\s+(p\w+)", text, re.M)
           if m.group(2) == "pName"}
    return kit


def parse_rect(literal):
    parts = [p.strip() for p in literal.split(",")]
    if len(parts) != 4:
        return None
    try:
        return tuple(int(p) for p in parts)
    except ValueError:
        return None


def controls(text):
    """[(name, builder, (l,t,r,b), line)] for every literal-rect builder call."""
    out = []
    for n, line in enumerate(text.split("\n"), 1):
        m = re.match(r'\s*(ui\w+)\s+"([^"]+)"\s*,\s*(.*)$', line)
        if not m:
            continue
        builder, name, rest = m.group(1), m.group(2), m.group(3)
        if builder not in RECT_ARG:
            continue
        args = [a.strip() for a in split_args(rest)]
        idx = RECT_ARG[builder] - 2      # rest begins at argument 2
        if idx < 0 or idx >= len(args):
            continue
        arg = args[idx]
        lit = re.fullmatch(r'"([-0-9,\s]+)"', arg)
        if not lit:
            continue                      # a computed rect; out of scope
        rect = parse_rect(lit.group(1))
        if rect:
            out.append((name, builder, rect, n))
    return out


def split_args(rest):
    """Split a builder's argument list on top-level commas only."""
    args, cur, in_str = [], [], False
    for c in rest:
        if c == '"':
            in_str = not in_str
        if c == "," and not in_str:
            args.append("".join(cur))
            cur = []
            continue
        cur.append(c)
    args.append("".join(cur))
    return args


def window_size(text):
    m = re.search(r'^\s*uiChrome\s+"[^"]*"\s*,\s*(\d+)\s*,\s*(\d+)', text, re.M)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None


def overlaps(a, b):
    return not (a[2] <= b[0] or b[2] <= a[0] or a[3] <= b[1] or b[3] <= a[1])


def contains(outer, inner):
    return (outer[0] <= inner[0] and outer[1] <= inner[1]
            and outer[2] >= inner[2] and outer[3] >= inner[3])


def main():
    problems, checked = [], 0
    kit = known_builders()

    unknown = sorted(kit - set(RECT_ARG) - NO_RECT)
    if unknown:
        problems.append(
            "the kit defines builder(s) this gate has no rect position for: %s. "
            "Add them to RECT_ARG, or their controls are silently unchecked."
            % ", ".join(unknown))
    stale = sorted((set(RECT_ARG) | NO_RECT) - kit)
    if stale:
        problems.append(
            "RECT_ARG names builder(s) the kit no longer defines: %s."
            % ", ".join(stale))

    for rel in DEMOS:
        path = os.path.join(ROOT, rel)
        if not os.path.exists(path):
            problems.append("%s: does not exist" % rel)
            continue

        text = demo_source(path)
        size = window_size(text)
        if size is None:
            problems.append("%s: no uiChrome call, so the window size is "
                            "unknown and nothing below can be checked" % rel)
            continue
        width, height = size

        if width > MAX_WIDTH or height > MAX_HEIGHT:
            problems.append(
                "%s: the window is %dx%d, past the family's 720p budget of "
                "%dx%d - a section below the bottom edge is a leg that does "
                "not get closed." % (rel, width, height, MAX_WIDTH, MAX_HEIGHT))

        found = controls(text)
        checked += len(found)
        if not found:
            problems.append("%s: no literal control rects found - the scan is "
                            "broken" % rel)
            continue

        window = (0, 0, width, height)
        panels = [(n, r) for n, b, r, _ in found if b in BACKGROUND_BUILDERS]

        for name, builder, rect, line in found:
            if rect[0] >= rect[2] or rect[1] >= rect[3]:
                problems.append(
                    "%s:%d: %s has an inside-out rect %s (left<right and "
                    "top<bottom, always)" % (rel, line, name, str(rect)))
                continue
            if not contains(window, rect):
                problems.append(
                    "%s:%d: %s at %s falls outside the %dx%d window - it "
                    "would be clipped, and nothing at runtime says so."
                    % (rel, line, name, str(rect), width, height))
                continue
            if builder in BACKGROUND_BUILDERS:
                continue
            # A control that sits on a panel must sit INSIDE it.
            for pname, prect in panels:
                if pname == name:
                    continue
                if overlaps(prect, rect) and not contains(prect, rect):
                    problems.append(
                        "%s:%d: %s at %s straddles the edge of panel %s at %s "
                        "- it would be drawn half on the card and half on the "
                        "page." % (rel, line, name, str(rect), pname, str(prect)))

        # The kit's builders are create-if-missing, so building one control
        # twice is legal and normal (a rebuild on reopen). Building it twice at
        # DIFFERENT rects is not: whichever call runs last wins, and the
        # control moves depending on which handler touched it - the failure the
        # kit's own "repaint from ONE place" rule exists to prevent.
        by_name = {}
        for name, builder, rect, line in found:
            if name in by_name and by_name[name][0] != rect:
                problems.append(
                    "%s:%d: %s is built at %s here and at %s on line %d - "
                    "whichever runs last wins. Give the rect ONE owner."
                    % (rel, line, name, str(rect), str(by_name[name][0]),
                       by_name[name][1]))
            by_name.setdefault(name, (rect, line))

        # Nothing but backgrounds may overlap. One entry per NAME, so an
        # idempotent rebuild is not read as a control overlapping itself.
        fg = [(n, r, l) for n, (r, l) in by_name.items()
              if n not in {p for p, _ in panels}]
        fg.sort(key=lambda e: e[2])
        for i in range(len(fg)):
            for j in range(i + 1, len(fg)):
                if overlaps(fg[i][1], fg[j][1]):
                    problems.append(
                        "%s:%d: %s at %s overlaps %s at %s (line %d)"
                        % (rel, fg[i][2], fg[i][0], str(fg[i][1]),
                           fg[j][0], str(fg[j][1]), fg[j][2]))

    for p in problems:
        print(p)
    if problems:
        print("\ncheck-demo-layout: %d problem(s)" % len(problems),
              file=sys.stderr)
        return 1
    print("check-demo-layout: OK (%d demo(s), %d control rect(s) checked)"
          % (len(DEMOS), checked))
    return 0


if __name__ == "__main__":
    sys.exit(main())
