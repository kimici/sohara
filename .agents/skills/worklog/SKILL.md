---
name: worklog
description: Record completed logical workspace changes with git commits and GitHub Pull Requests, and keep only a focused workstate before changes are committed or synced. Use after a logical change that edits, creates, deletes, or reconfigures workspace files, or when the user asks about progress, recent changes, or plans.
---

# Worklog

The durable worklog is version-control history, not a per-day file. Git
commits record individual completed changes; a GitHub Pull Request is the
reviewable, aggregated record of a body of work. Before anything is
committed or synced, keep only a compact, focused workstate.

## Rules

- Follow the repository's explicit convention (see `AGENTS.md`) when present.
- **Never** create or append `.agents/worklog/YYYY-MM-DD.md` daily files. Git
  history is the change record; historical daily files are unmaintained
  archives, not the live log.
- Work on a feature branch. Make **one commit per completed logical change**,
  not per command or per file edit, and only after that increment's acceptance
  checks pass.
- Write a descriptive commit subject and a body that records what changed,
  why, and the verification evidence. This replaces the old daily entry.
- For a body of work, use a GitHub Pull Request (`gh pr create` / `gh pr
  edit`) as the aggregated worklog: the PR description carries the summary,
  the increments, the evidence, and the remaining gaps. Link the PR from
  status reports.
- **Uncommitted work**: keep `.agents/workstate.md` as a compact snapshot of
  ONLY the current focus — what is being worked on, the immediate next step,
  and any blocker. No timelines, no historical sections, no append-only
  history, no duplicate of commit/PR content. Replace stale content instead
  of preserving it; once a change is committed, move the focus to the next
  increment.
- Keep entries concise; do not log intermediate attempts or command
  transcripts.

## Progress questions

When asked about progress, recent changes, or what is planned next, read
`git log`, `gh pr view`, and the focused workstate — do not look for daily
log files.

## Output

In the user's language, state which commit (and PR, if any) records the
change, what the focused workstate now says, and any verification evidence or
remaining gap.
