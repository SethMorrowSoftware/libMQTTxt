# Vendored from xtalk-suite

libMQTTxt is a **standalone repository**; the xTalk suite's carried-block
masters live in another one. Two of them are copied here so this repo's demo
can carry them and so the drift gates have something local to compare against.

| File | Upstream path | Source |
|------|---------------|--------|
| `tools/ui-kit.livecodescript` | `tools/ui-kit.livecodescript` | [SethMorrowSoftware/xtalk-suite](https://github.com/SethMorrowSoftware/xtalk-suite) @ `01225cc` |
| `tools/demo-selfcheck.livecodescript` | `tools/demo-selfcheck.livecodescript` | [SethMorrowSoftware/xtalk-suite](https://github.com/SethMorrowSoftware/xtalk-suite) @ `01225cc` |

Pinned commit: `01225cc9b513b906b6ab95b41dfe0fb499d1bf4e`

## What this coupling actually is, and what it is not

Inside the suite these files are masters, and their gates hold every copy in
the monorepo byte-identical. Here they are **vendored copies**, and the gates in
this repo hold only *this repo's* demo identical to *this copy*. Nothing
automatic connects the two repositories.

So the honest statement is: the demo cannot silently drift from the vendored
kit, and the vendored kit **can** silently fall behind upstream. That second
half is a manual step, deliberately:

```sh
# re-sync from upstream, then re-carry into the demo
cp ../xtalk-suite/tools/ui-kit.livecodescript        tools/
cp ../xtalk-suite/tools/demo-selfcheck.livecodescript tools/
python3 tools/sync-demo-embeds.py     # re-carries every block into the demo
python3 tools/check-ui-kit-drift.py   # and the gates confirm it
python3 tools/check-demo-selfcheck-drift.py
```

Update the commit above in the same change, or this table becomes the kind of
hand-copied number the suite's own notes warn about.

## Why vendor rather than submodule

A submodule would make the demo un-pasteable without a checkout of a second
repository, which defeats the one rule these blocks exist to serve: **a demo is
one file you paste and open.** The vendored copy keeps that true.
