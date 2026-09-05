#!/usr/bin/env python3
"""sync-demo-embeds.py - the demo carries the library it needs.

WHY THIS EXISTS
    A demo that requires `start using stack "libMQTTxt.oxtstack"` before it
    will run is a demo most people meet as an error message. The library is
    pure script, so there is no reason a reader should have to wire it up by
    hand just to look at the thing: paste the demo into a stack script, open
    it, and it works. One file per demo, no wiring.

    Both forms stay usable. `libMQTTxt.oxtstack` remains the single source of
    truth and the right dependency for a real project; this tool copies it into
    the demo between sentinels so the SHIPPED demo is self-contained. Nobody
    hand-edits inside the sentinels, and `--check` fails the build when the
    copy drifts.

    Ported from xtalk-suite's tools/sync-demo-embeds.py (see tools/VENDORED.md)
    and cut down to one demo and one provider - the suite's registry exists to
    order multi-library embeds, and this repo has one library.

ORDER IS LOAD-BEARING, NOT COSMETIC
    The embedded block goes ABOVE the demo's own code. OXT resolves
    script-level `constant` and `local` names by LEXICAL POSITION, and an
    undeclared name evaluates to the literal text of its own name - so a
    provider placed below its first reader produces a tidy wrong answer rather
    than an error.

THE ENGINE'S SOCKET MESSAGES RIDE ALONG HERE
    libMQTTxt declares `on socketClosed` / `on socketError` / `on socketTimeout`.
    The suite's tool can DROP those wrappers for an embedder that defines its
    own three and calls the library's named functions instead. This demo
    defines none, so the wrappers must ride along - dropping them with nothing
    else listening leaves the library's sockets unattended, which is a silent
    hang rather than an error. The check below asserts they survived, so that
    can never become an accident.

COLLISIONS ARE REFUSED, NEVER MERGED
    If the demo and the library define the same handler, or the same column-0
    declaration, the merged script would not compile - and the maintainer would
    meet that at PASTE TIME on an engine. So this tool refuses to write, names
    both sides, and stops.

THE BANNER DELIBERATELY AVOIDS ONE PHRASE
    check-ui-kit-drift.py and check-demo-selfcheck-drift.py SKIP any file whose
    first 4000 characters contain "GENERATED - do not edit". This demo is only
    PARTLY generated - the regions between markers - so bannering it with that
    phrase would silently switch off drift checking for the two blocks it
    carries. The header written below says the same thing in words that do not
    trip that test, and this paragraph exists so nobody tidies it back.

USAGE
    python3 tools/sync-demo-embeds.py            # write the embed
    python3 tools/sync-demo-embeds.py --check    # verify the committed copy
    Exit 0 when clean, 1 when the copy is stale or a collision is found.
"""

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BEGIN = "-- >>> BEGIN EMBEDDED LIBRARIES (tools/sync-demo-embeds.py) >>>"
END = "-- <<< END EMBEDDED LIBRARIES <<<"

# demo -> the libraries it carries, IN DEPENDENCY ORDER.
REGISTRY = {
    os.path.join("examples", "mqtt-dashboard.livecodescript"): [
        "libMQTTxt.oxtstack"],
}

# The engine's socket messages the library declares. Not dropped for any
# carrier here, and asserted present after every write: see the header.
ENGINE_SOCKET_MESSAGES = ("socketClosed", "socketError", "socketTimeout")

DEF = re.compile(
    r'^(?:private\s+)?(?:command|function|on|getprop|setprop)\s+(\w+)', re.M)
DECL = re.compile(r'^(?:local|constant|global)\s+(.+)$', re.M)
SCRIPT_HEADER = re.compile(r'^script\s+"[^"]*"[^\n]*\n', re.M)


def names(text):
    """Script-level handler and declaration names. Column 0 only: an indented
    `local` is a HANDLER local, scoped to its handler, and cannot collide.

    THE TRAILING COMMENT COMES OFF FIRST. Nearly every declaration in this
    family carries one, and a checker that requires the remainder of the line
    to be a bare identifier is blind to all of them - which is how a duplicate
    `local sPolling` once reached an engine as a hard compile error in the
    suite. Comments off, then the value, then commas.
    """
    handlers = set(DEF.findall(text))
    decls = set()
    for line in DECL.findall(text):
        line = line.split("--", 1)[0].split("#", 1)[0]
        # `=` BEFORE `,`: `constant kDeck = "Ac,Ad,Ah"` split the other way
        # round yields `Ad` and `Ah`, which look exactly like identifiers.
        line = line.split("=", 1)[0]
        for part in line.split(","):
            n = part.strip()
            if re.fullmatch(r"[A-Za-z_]\w*", n):
                decls.add(n)
    return handlers, decls


def strip_script_header(text, rel):
    """Drop a provider's leading `script "Name"` line before embedding it.

    A `script "..."` line is a stack-NAME marker, meaningful only when the file
    IS its own stack. Embedding one verbatim drops a SECOND marker into the
    middle of a demo that already carries its own on line 1, naming a stack
    that is not there. The strip is unconditional and asserted below.
    """
    return SCRIPT_HEADER.sub("", text, count=1)


def build_block(demo_rel, providers):
    """The text that goes between the sentinels, or (None, error)."""
    out = [
        BEGIN,
        "-- Pasted from the source below so this demo runs with NO `start",
        "-- using` wiring. Do not edit inside these sentinels: run",
        "-- `python3 tools/sync-demo-embeds.py` instead. The source stays the",
        "-- single point of truth and the right dependency for a real project.",
        "--",
        "-- The library is placed ABOVE the demo's own code because OXT resolves",
        "-- script-level declarations by lexical position.",
        "",
    ]

    demo_path = os.path.join(ROOT, demo_rel)
    with open(demo_path, encoding="utf-8") as fh:
        demo_text = fh.read()

    # The demo's own names, with the carried regions cut out - a region is not
    # the demo's code and its names belong to whoever mastered it.
    own = cut_spans(demo_text)
    demo_handlers, demo_decls = names(own)

    seen_handlers, seen_decls = set(), set()
    for rel in providers:
        path = os.path.join(ROOT, rel)
        if not os.path.exists(path):
            return None, "%s: provider %s does not exist" % (demo_rel, rel)
        with open(path, encoding="utf-8") as fh:
            text = fh.read()

        text = strip_script_header(text, rel)
        if SCRIPT_HEADER.search(text):
            return None, ("%s: a `script \"...\"` line survived the strip in %s"
                          % (demo_rel, rel))

        p_handlers, p_decls = names(text)

        clash = sorted((p_handlers & demo_handlers) | (p_handlers & seen_handlers))
        if clash:
            return None, ("%s: %s and the demo (or an earlier provider) both "
                          "define handler(s): %s. The merged script would not "
                          "compile; rename one side."
                          % (demo_rel, rel, ", ".join(clash)))
        clash = sorted((p_decls & demo_decls) | (p_decls & seen_decls))
        if clash:
            return None, ("%s: %s and the demo (or an earlier provider) both "
                          "declare: %s. Two script-level declarations of one "
                          "name is a hard compile error; rename one side."
                          % (demo_rel, rel, ", ".join(clash)))

        seen_handlers |= p_handlers
        seen_decls |= p_decls

        out.append("-- ---- %s ----" % rel)
        out.append(text.rstrip("\n"))
        out.append("")

    # The wrappers must have survived. Nothing drops them in this repo, so a
    # missing one means the library stopped declaring it - and the demo's
    # sockets would go quiet with no error.
    block = "\n".join(out)
    for msg in ENGINE_SOCKET_MESSAGES:
        if not re.search(r"^on\s+%s\b" % msg, block, re.M):
            return None, ("%s: the embedded library no longer declares `on %s`. "
                          "This demo defines no socket handlers of its own, so "
                          "nothing would be listening - a silent hang, not an "
                          "error." % (demo_rel, msg))

    out.append(END)
    return "\n".join(out), None


SPANS = [(">>> BEGIN EMBEDDED LIBRARIES", "<<< END EMBEDDED LIBRARIES"),
         ("==== SUITE UI KIT v2 BEGIN", "==== SUITE UI KIT v2 END"),
         ("==== DEMO SELF-CHECK v1 BEGIN", "==== DEMO SELF-CHECK v1 END")]


def cut_spans(text):
    """The demo's OWN code: every carried region removed."""
    keep, skip = [], None
    for line in text.split("\n"):
        if skip is None:
            hit = next((s for s in SPANS if s[0] in line), None)
            if hit:
                skip = hit[1]
                continue
            keep.append(line)
        elif skip in line:
            skip = None
    return "\n".join(keep)


def splice(text, block, demo_rel):
    """Replace the sentinel region, or (None, error)."""
    start = text.find(BEGIN)
    if start < 0:
        return None, "%s: no BEGIN sentinel" % demo_rel
    end = text.find(END, start)
    if end < 0:
        return None, "%s: BEGIN sentinel with no END" % demo_rel
    return text[:start] + block + text[end + len(END):], None


def main(argv):
    check = "--check" in argv
    problems, wrote = [], 0

    for demo_rel, providers in sorted(REGISTRY.items()):
        demo_path = os.path.join(ROOT, demo_rel)
        if not os.path.exists(demo_path):
            problems.append("%s: does not exist" % demo_rel)
            continue

        block, err = build_block(demo_rel, providers)
        if err:
            problems.append(err)
            continue

        with open(demo_path, encoding="utf-8") as fh:
            text = fh.read()
        new, err = splice(text, block, demo_rel)
        if err:
            problems.append(err)
            continue

        if new == text:
            continue
        if check:
            problems.append(
                "%s: the embedded copy is STALE. Run "
                "`python3 tools/sync-demo-embeds.py`." % demo_rel)
            continue
        with open(demo_path, "w", encoding="utf-8") as fh:
            fh.write(new)
        wrote += 1
        print("%s: embedded %s" % (demo_rel, ", ".join(providers)))

    for p in problems:
        print(p, file=sys.stderr)
    if problems:
        print("sync-demo-embeds: %d problem(s)" % len(problems), file=sys.stderr)
        return 1

    if check:
        print("sync-demo-embeds: OK (%d demo(s), embedded copies current)"
              % len(REGISTRY))
    else:
        print("sync-demo-embeds: OK (%d written, %d already current)"
              % (wrote, len(REGISTRY) - wrote))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
