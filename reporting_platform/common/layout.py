"""Where the feed registry lives on disk, and what its filenames mean.

SEPARATE FROM `context.py` FOR THE REASON `settings.py` IS. Three callers need
these rules and none of them should have to import the others: `context` reads
the tree, `ui/registry` writes it, and `tests/support` builds throwaway copies
of it. A module whose only imports are stdlib and ruamel can be imported by all
three, and by the one-shot migration script.

THE LAYOUT. One file per feed, one per convention, one for the defaults tier:

    <CONFIG_DIR>/feeds/
        _defaults.yml            the `defaults:` tier -- the mapping itself
        conventions/
            ref_src.yml          one convention -- the settings mapping itself
        fo_trade.yml             one feed -- the block itself, `name:` included

No wrapper keys. A feed file IS the block that used to sit under `feeds:`, a
convention file IS the mapping that used to sit under its name. Re-stating the
key inside the file it names is the kind of redundancy that drifts.

THE FILENAME IS THE IDENTITY, and `fo_trade.yml` must declare `name: fo_trade`.
Not tidiness: that one string is the raw table, the DAG id, the landing prefix,
the dbt source table and the prepared model at once
(docs/DECISIONS.md#feed-names-carry-the-source), and a file whose name disagrees
with its `name:` puts a sixth spelling into play that nothing reconciles. The
two would diverge the first time somebody copied a file to start a new feed --
which is how a new feed actually gets written.

IT IS ALSO WHAT MAKES DUPLICATE NAMES IMPOSSIBLE. The old `feeds:` list was
built with `out[block["name"]] = Feed(...)`, so two blocks sharing a name
collapsed into one entry with the last winning and nothing raising -- the exact
failure `#feed-conventions` names as the reason a convention may not set
`name:`, guarded there and unguarded here. A directory cannot hold two files
with one name, so the filesystem now enforces what no check did.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

FEEDS_DIR = "feeds"
CONVENTIONS_DIR = "conventions"
DEFAULTS_FILE = "_defaults.yml"

# What a feed or convention may be called. The same shape ADDING-A-FEED.md has
# always asked for ("lowercase with underscores regardless of what the upstream
# calls its files") -- unenforced while the name was a value in a list, and
# enforceable now that it is a filename. A name reaches a table, a DAG id, an
# S3 prefix and a Python identifier in dbt, and the intersection of what those
# accept is narrower than what a filesystem does.
NAME = re.compile(r"[a-z][a-z0-9_]*")


def feeds_root(config_dir: Path) -> Path:
    return Path(config_dir) / FEEDS_DIR


def conventions_root(config_dir: Path) -> Path:
    return feeds_root(config_dir) / CONVENTIONS_DIR


def defaults_path(config_dir: Path) -> Path:
    return feeds_root(config_dir) / DEFAULTS_FILE


def _check_name(path: Path, kind: str) -> str:
    """The name `path` declares, or raise saying why it cannot be one.

    Checked on the way IN, so a badly named file is an error that names the
    file rather than a feed that exists under a name nothing else can spell.
    """
    stem = path.stem
    if not NAME.fullmatch(stem):
        raise ValueError(
            f"{path}: {kind} filenames are the {kind}'s NAME, so this one has "
            f"to be a legal name: lowercase letters, digits and underscores, "
            f"starting with a letter. {stem!r} is not. The name becomes a "
            f"table, a DAG id and an S3 prefix, not just a file.")
    return stem


def feed_paths(config_dir: Path) -> list[Path]:
    """Every feed file, sorted. `_`-prefixed files are not feeds.

    RAISES rather than returning empty when the directory is absent, for the
    reason `models_in` does: a container without the config mounted would
    otherwise report that the platform ingests nothing, and every sweep and
    monitor would succeed having looked at no feeds at all.
    """
    root = feeds_root(config_dir)
    if not root.is_dir():
        raise RuntimeError(
            f"no feed registry at {root}. The platform derives its tables, "
            f"DAGs and landing prefixes from it, so an absent one is not an "
            f"empty one -- set REPORTING_CONFIG_DIR, or mount the config "
            f"directory into this service.")
    out = sorted(p for p in root.glob("*.yml") if not p.name.startswith("_"))
    for path in out:
        _check_name(path, "feed")
    return out


def convention_paths(config_dir: Path) -> list[Path]:
    """Every convention file, sorted. An absent directory means none."""
    root = conventions_root(config_dir)
    if not root.is_dir():
        return []
    out = sorted(p for p in root.glob("*.yml") if not p.name.startswith("_"))
    for path in out:
        _check_name(path, "convention")
    return out


def registry_files(config_dir: Path) -> tuple[tuple[str, int], ...]:
    """(relative path, mtime_ns) for everything a resolved Feed depends on.

    THE CACHE KEY, and it covers ALL THREE TIERS. A `Feed` is `_defaults.yml`
    overlaid with its convention overlaid with its own block, so an edit to any
    of them must invalidate it.

    NOT THE DIRECTORIES' MTIMES. A directory's mtime moves when a file is added
    or removed and NOT when one is edited. That is fine for `models_in`, whose
    set only changes on add/remove, and wrong here -- and the way it would fail
    is a convention edited, no feed changing, and nothing reporting an error:
    exactly the bug `_load`'s mtime key was written to kill, in a new shape.

    Costs one `stat` per file. Measured at 40 feeds: 115us to build this key
    against 3.5us for a single stat -- against the 0.17s `lineage/columns.py`
    already spends tracing one model's columns, it is not a number worth
    optimising.
    """
    root = feeds_root(config_dir)
    paths = [*root.glob("*.yml"), *conventions_root(config_dir).glob("*.yml")]
    return tuple(sorted((str(p.relative_to(root)), p.stat().st_mtime_ns)
                        for p in paths))


def check_declares_its_name(path: Path, block: Any, kind: str = "feed",
                            expect_name: bool = True) -> str:
    """The name `path` carries, with the block cross-checked against it.

    `expect_name` is False for a CONVENTION, which may not carry `name:` at
    all -- not because the filename would disagree but because a convention
    setting `name:` means something else entirely (it would supply that name
    to every feed inheriting it, collapsing them into one registry entry).
    That rule and its explanation already live in `context.CONVENTION_FORBIDDEN`
    and stay there; this only declines to pre-empt them with a worse message.
    """
    stem = _check_name(path, kind)
    if not isinstance(block, dict):
        raise ValueError(
            f"{path}: a {kind} file must be a mapping of settings, got "
            f"{type(block).__name__}. There is no wrapper key -- the filename "
            f"is the name.")
    declared = block.get("name")
    if expect_name and declared is not None and str(declared) != stem:
        raise ValueError(
            f"{path}: declares `name: {declared}` but is named {stem!r}. The "
            f"filename is the identity, so these cannot differ -- rename the "
            f"file, or fix the `name:`.")
    return stem


# ------------------------------------------------------------------ splitting
# Used twice: by the one-shot migration off the single `feeds.yml`, and by
# `tests/support.config_dir()`, which still takes a fixture as one document
# because fifty call sites write one and a fixture is easier to read whole.
# ONE implementation, so a fixture cannot be split by different rules than the
# real config was.

def _yaml():
    from ruamel.yaml import YAML
    y = YAML()
    y.preserve_quotes = True
    y.width = 4096            # never re-wrap a pattern or a description
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def _comment_tokens(node):
    """Every comment token hanging off `node` and its children."""
    ca = getattr(node, "ca", None)
    if ca is not None:
        groups = list(ca.items.values())
        if getattr(ca, "comment", None):
            groups.append(ca.comment)
        for group in groups:
            for slot in (group if isinstance(group, list) else [group]):
                for tok in (slot if isinstance(slot, list) else [slot]):
                    if tok is not None and hasattr(tok, "start_mark"):
                        yield tok
    children = (node.values() if isinstance(node, dict)
                else node if isinstance(node, list) else ())
    for child in children:
        yield from _comment_tokens(child)


def _dedent_comments(node, delta: int) -> None:
    """Shift every comment under `node` left by `delta` columns.

    A BLOCK THAT MOVES TO TOP LEVEL TAKES ITS COMMENTS' INDENTATION WITH IT.
    ruamel keeps the text of a comment in the token and its column in the
    token's `start_mark`, so a note written at column 4 inside a list item
    re-emits at column 4 in a file whose keys start at column 0. It still
    parses -- YAML lets a comment sit anywhere -- which is why this is worth
    doing deliberately: the migration would otherwise produce, losslessly and
    silently, a tree of files that all look broken.
    """
    for tok in _comment_tokens(node):
        tok.start_mark.column = max(0, tok.start_mark.column - delta)
        # TWO PLACES CARRY THE INDENT, not one. A comment on its own line
        # before a key keeps its column in `start_mark`; a comment that
        # ruamel captured as trailing the PREVIOUS key holds the newline and
        # the spaces inside the token's own text. Fixing only the first
        # dedents some comments in a file and not others, which is worse
        # than fixing neither.
        if "\n" in tok.value:
            tok.value = "\n".join(
                line[delta:] if line[:delta].isspace() else line
                for line in tok.value.split("\n"))


def _dump(block, path: Path, y) -> None:
    import io
    buf = io.StringIO()
    y.dump(block, buf)
    path.write_text(buf.getvalue().rstrip() + "\n", encoding="utf-8")


def split_document(text: str, config_dir: Path) -> dict[str, int]:
    """Write the one-document form at `text` out as the per-file tree.

    ROUND-TRIP, NOT RE-EMIT, for the reason `ui/registry` is: feeds.yml is more
    comment than data and the comments are the reasoning. A block's own
    comments travel with it because ruamel hangs them off the mapping; the
    comment written ABOVE a block hangs off the sequence instead, so it is
    moved across explicitly below -- without that, every "why this feed is
    like this" note would be dropped on the floor by the migration that was
    supposed to be lossless.
    """
    y = _yaml()
    doc = y.load(text)
    root = feeds_root(config_dir)
    root.mkdir(parents=True, exist_ok=True)

    written = {"feeds": 0, "conventions": 0, "defaults": 0}

    if doc.get("defaults"):
        # `defaults:` keys sit at column 2, a feed block's and a
        # convention's at column 4 -- the nesting each one is losing.
        _dedent_comments(doc["defaults"], 2)
        _dump(doc["defaults"], defaults_path(config_dir), y)
        written["defaults"] = 1

    section = doc.get("conventions") or {}
    if not isinstance(section, dict):
        raise ValueError(
            f"conventions must be a mapping of name to settings, got "
            f"{type(section).__name__} -- each one becomes "
            f"{FEEDS_DIR}/{CONVENTIONS_DIR}/<name>.yml, and a list has no "
            f"names to make filenames out of.")
    if section:
        conventions_root(config_dir).mkdir(parents=True, exist_ok=True)
        for cname, settings in section.items():
            _dedent_comments(settings, 4)
            _dump(settings, conventions_root(config_dir) / f"{cname}.yml", y)
            written["conventions"] += 1

    blocks = doc.get("feeds") or []
    for i, block in enumerate(blocks):
        # The comment ABOVE this block, which ruamel attached to the sequence
        # rather than to the mapping. `.ca.items[i]` is a 4-slot list and the
        # pre-comment is slot 1 for a sequence item.
        _dedent_comments(block, 4)
        pre = blocks.ca.items.get(i) if hasattr(blocks, "ca") else None
        if pre and pre[1]:
            tokens = [t for t in pre[1] if t is not None]
            text_above = "".join(t.value for t in tokens)
            # Drop the blank-line separators `ui/registry.add` inserts; keep
            # anything that is actually a comment.
            kept = [ln for ln in text_above.splitlines() if ln.strip()]
            if kept:
                block.yaml_set_start_comment("\n".join(
                    ln.lstrip().lstrip("#").strip() for ln in kept))
        _dump(block, root / f"{block['name']}.yml", y)
        written["feeds"] += 1
    return written
