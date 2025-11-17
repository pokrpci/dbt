<!-- .github/copilot-instructions.md -->
Purpose
-------
These instructions give AI coding agents the minimal, high-value context to be productive in this dbt repository. Focus on where to make changes, how to run/dbt commands to validate them, and where to look for examples.

Quick snapshot (what this repo is)
- A dbt project stored under `pci_dbt/` (see `pci_dbt/dbt_project.yml`).
- Uses local package vendor code under `pci_dbt/dbt_packages/` (notably `dbt_utils`).
- Typical dbt artifacts are built into `pci_dbt/target/` (compiled SQL, manifest, run_results).

Key files & folders (read first)
- `pci_dbt/dbt_project.yml` — project config and model path mappings.
- `pci_dbt/profiles.yml` — environment/connection config included in the repo (this project keeps a profiles.yml in-tree).
- `pci_dbt/packages.yml` — package dependencies; run `dbt deps` after edits.
- `pci_dbt/macros/` — reusable SQL/Jinja macros (e.g. `dateadd.sql`, `load_to_sqlserver.sql`).
- `pci_dbt/models/` — models are organized by provider and layers. Example structure:
  - `models/SQLServer/intermediate/` — staging SQL for SQL Server sources (stg_*.sql)
  - `models/SQLServer/stage/` — higher-level staging
  - `models/dbt/` — project-specific models (`my_ep_dbt_model.sql`, `USER_LOGIN.sql`)
- `pci_dbt/seeds/Interaction.csv` — example seed file.
- `pci_dbt/target/manifest.json` and `run_results.json` — used to debug runs and tests.
- `logs/` and `pci_dbt/logs/` — dbt log files (dbt.log.*) for runtime debugging.

Developer workflows (commands you will use)
(These are the canonical commands to validate changes. Prefer running them locally before proposing changes.)
- Install deps: `dbt deps` (ensure `pci_dbt/packages.yml` is up-to-date).
- Quick check: `dbt debug` (verify connection & profile).
- Run models: `dbt run` or `dbt run --models <selector>` (use model name or path). Example: `dbt run -m dbt.my_ep_dbt_model`.
- Run tests: `dbt test` (or `dbt test -m <models>`).
- Load seeds: `dbt seed` (uses `pci_dbt/seeds/`).
- Compile only: `dbt compile` (inspect compiled SQL under `target/compiled/`).
- Generate docs: `dbt docs generate` and `dbt docs serve`.

Patterns & conventions (project-specific)
- Model layout: models are grouped by database/provider (see `models/SQLServer/`) and by layer (stage/intermediate/target). Keep new models under the appropriate provider folder.
- Naming: staging models are prefixed `stg_` (see many `stg_*.sql` files in `models/SQLServer/intermediate/`).
- Schema and sources: `schema.yml` and `sources.yml` live next to models; they define tests and source mappings — update these when adding models or sources.
- Macros: small, single-responsibility macros live in `pci_dbt/macros/`. If reusing functionality, add a macro rather than copy-pasting SQL.
- Packages: vendor code is vendored under `pci_dbt/dbt_packages/`. Do not edit package internals — update `pci_dbt/packages.yml` instead and run `dbt deps`.

Debugging tips & where to look
- When runs fail, check: `pci_dbt/target/run_results.json` and `pci_dbt/target/manifest.json` for failure context.
- Review `pci_dbt/target/compiled/dbt_project/` to inspect generated SQL.
- Check logs in `c:\Program Files\dbt\logs\` and `c:\Program Files\dbt\pci_dbt\logs\` for dbt runtime traces.

What AI agents should and should not edit
- SHOULD edit: files under `pci_dbt/models/`, `pci_dbt/macros/`, `pci_dbt/seeds/`, and `pci_dbt/snapshots/` for feature work.
- MAY update: `pci_dbt/packages.yml` (and run `dbt deps`) or `pci_dbt/dbt_project.yml` for model path/config changes — but note these change project behavior and should include a brief rationale in PR description.
- DO NOT edit: files under `pci_dbt/target/` or `pci_dbt/dbt_packages/` (these are build artifacts and vendored package contents).

Examples (point to concrete files)
- To add a new staging model for a SQL Server source, follow examples in `pci_dbt/models/SQLServer/intermediate/` (stg_*.sql) and update `pci_dbt/models/SQLServer/sources.yml` and `schema.yml` next to the model.
- To change date handling use macros in `pci_dbt/macros/dateadd.sql` and `date_differences.sql` as canonical helpers.

If something is missing
- If you need credentials or CI details not in-tree, ask a human (the repo keeps a `profiles.yml` here, but CI secrets are likely external).

Feedback
- If any part of this guidance is unclear or you need more examples (tests, CI run commands, or naming exceptions), tell me which area and I'll expand the file with concrete snippets.
