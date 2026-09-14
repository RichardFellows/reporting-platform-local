# `.claude/worktrees/` is not ignored

**Value** low · **Effort** 5 minutes · **Branch** `chore/ignore-claude-worktrees`

## What is wrong (verified 2026-09-14)

An agent session run with worktree isolation checks the repo out under
`.claude/worktrees/<id>/`, inside the primary checkout. Nothing ignores it:

```bash
git worktree add -q .claude/worktrees/probe HEAD && git status --porcelain; \
  git worktree remove .claude/worktrees/probe
#  ?? .claude/
git ls-files .claude | wc -l
#  0
```

Once the worktree is removed the directory is empty and `git status` goes
clean again, which is why it is easy to miss: it only shows while agents are
running.

## Why it matters

While it shows, every `git status` in the primary checkout is dirty, which
hides a real untracked file, and a `git add -A` or `git add .` stages a nested
checkout. It also puts whole copies of the repo in the path of any
repo-root `grep -r` — a search for `keep_years: 8` returned hits from two
agents' worktrees during the session that found this.

## What done looks like

- [ ] `.gitignore` gains `.claude/worktrees/`, with a comment saying what
      creates it.
- [ ] The command above prints nothing.

## Watch out for

Ignore `.claude/worktrees/`, **not** `.claude/`. Nothing under `.claude/` is
tracked today, but project settings, agents and skills live there and are
meant to be committable.

## Prompt for a new session

```text
Read docs/todo/11-claude-worktrees-are-not-ignored.md. Add .claude/worktrees/
to .gitignore -- not .claude/ -- and show the verification command printing
nothing.
```
