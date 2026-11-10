# AGENTS.md — Infra-Signal autonomous engineering operating contract

## Purpose
This repository is part of the Hammett Enterprise engineering portfolio. The Chairman wants safe forward progress on previously delegated work **without repeating “continue”**. These instructions govern each Codex/AI engineering session once actually started or scheduled; this file **does not itself launch a background agent or spend credits**.

## Source of truth
- Read current branch, changes, open issues/PRs, test results, and actual source code before planning.
- Prioritize Issue #1. Phase 11C baseline, customer-safe demo readiness, source reliability, tests, and verified operational claims. Do not access or alter real customer resources.
- Link concrete source evidence for status. Never assume a prior chat's claims, deploys, tests or availability are current.
- Do not duplicate GitHub issues or start a parallel Command Center. AHIP is the eventual portfolio authority.

## Execution loop for each authorized session
1. Inspect repository status. Preserve uncommitted work and active worktrees.
2. Pick the **single highest-value unblocked** issue-sized increment favoring reliability, launch, revenue or risk.
3. Execute safe work within the session: focused code/tests/docs or reproducible investigation, not just a high-level plan.
4. Validate with relevant targeted tests and static checks. Mark unrun checks explicitly; never fabricate success.
5. Push changes only to a new issue-scoped feature branch when repository write authorization exists. Prepare a **draft PR**, never a direct production change.
6. Update the issue or PR with exact evidence, limitations, and the next unblocked step. If one item blocks, move to another safe backlog item instead of pausing all work.
7. End with a concise exception-only report: what changed, verifiable link, test result, next step, and decision only if absolutely necessary.

## Decision and budget boundaries
- **Allowed without another routine 'continue':** repository inspection, technical analysis, test planning, small isolated code/test/docs changes, draft PRs and backlog updates where tools/permissions permit.
- **Require explicit task-specific approval:** merges into protected branches, releases/deployments, production data/tenant or cloud changes, credentials/OAuth scopes, paid model/API use or top-ups, external communications, publishing to social channels, CRM mutations, customer bids/contracts, destructive or irreversible actions.
- Never retry rate limits indefinitely; conserve Codex/Sintra/HeyGen credits and any infrastructure allowances. If the actual credit balance is unknown, say so and do not assume unlimited capacity.
- Use approved official integrations, least privilege, verified account identity and tenant/workspace boundaries. No bypasses, tokens in source, invented metrics or misleading 'connected' claims.
- Review existing test/quality gates before modifying workflow files. Never weaken gates to force a pass.

## Continuity
This file is **instructional**, not an always-on scheduler. Actual recurring execution requires a configured ChatGPT Work/Codex Cloud task, a connected agent, or another approved automation. On restart, read this file, the issues and the last draft PR; resume the highest priority safe next step without requesting a new instruction.
