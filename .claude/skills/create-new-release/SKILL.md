---
name: create-new-release
description: Create and push a new semver release tag. Asks for release type (patch/minor/major), verifies META.json version, verifies pg_stat_ch.control's default_version is aligned with distribution major.minor (Theory's policy), confirms the canonical SQL file and any required migration script are in place, validates HEAD is on remote, then pushes the tag and reminds the operator of the downstream pgext-packaging / AMI-build / clickgres-platform handoffs.
---

# Create New Release

Create and push a new semantic version release tag.

## Steps

1. **Ask release type**: Use AskUserQuestion to ask if this is a patch, minor, or major release.

2. **Fetch latest tags from remote**:
   ```bash
   git fetch --tags origin
   ```

3. **Get the latest semver tag** (assumes `v` prefix like `v1.2.3`):
   ```bash
   git tag --sort=-version:refname -l 'v*' | head -1
   ```

4. **Parse and bump version** based on user selection:
   - **patch**: `v1.2.3` → `v1.2.4`
   - **minor**: `v1.2.3` → `v1.3.0`
   - **major**: `v1.2.3` → `v2.0.0`

5. **Verify META.json version matches the new tag**:
   Read `META.json` and check that both `version` and `provides.pg_stat_ch.version` match the new version (without the `v` prefix). If they don't match, abort and tell the user to update `META.json` first — PGXN uses this file for the release version.

6. **Verify extension `default_version` is aligned with the distribution major.minor** (Theory's policy from PR #33):
   The extension version in `pg_stat_ch.control` (`default_version`) must equal the new tag's `MAJOR.MINOR`. So a `v0.3.7` tag requires `default_version = '0.3'`; a `v0.4.0` tag requires `default_version = '0.4'`.

   ```bash
   ext_version=$(grep -E "^default_version" pg_stat_ch.control | sed -E "s/.*'([^']+)'.*/\1/")
   want="<MAJOR>.<MINOR>"   # derived from the new tag
   ```

   If `ext_version != want`, abort and tell the user the alignment is wrong. The fix depends on the bump type:

   - **Patch bump** (e.g., 0.3.6 → 0.3.7): `default_version` should already equal the existing major.minor. If it doesn't, the previous release was misaligned — that's a separate fix (the user has to land a PR that bumps `default_version`, renames the canonical SQL file `pg_stat_ch--<old>.sql` → `pg_stat_ch--<new>.sql`, and adds a migration `pg_stat_ch--<prev_default>--<new>.sql` for existing installs). See PR #84 for a precedent.
   - **Minor or major bump** (e.g., 0.3.x → 0.4.0): the bump itself changes major.minor, so the user must, in the *same release*, also: (a) bump `default_version` to the new major.minor, (b) rename `sql/pg_stat_ch--<old>.sql` → `sql/pg_stat_ch--<new>.sql`, (c) add `sql/pg_stat_ch--<prev_default>--<new>.sql` doing whatever DDL is required to migrate from the prior shape (or a no-op `DO $$ ... $$` block guarded on catalog state if the SQL surface didn't change).

   Do not tag the release until alignment holds.

7. **Verify the canonical SQL file and migration script are in place**:
   ```bash
   ls sql/pg_stat_ch--<ext_version>.sql                # canonical install file
   ```
   The canonical SQL file `sql/pg_stat_ch--<ext_version>.sql` must exist (e.g., `sql/pg_stat_ch--0.3.sql` for `default_version = '0.3'`).

   If `default_version` was bumped in this release relative to the previous tag, also verify:
   ```bash
   ls sql/pg_stat_ch--<prev_ext_version>--<ext_version>.sql
   ```
   The migration script for the prior `default_version` must exist. If either file is missing, abort and tell the user to add them (see PR #84 for the conditional-DDL pattern that handles both legacy and current catalog shapes).

8. **Validate current commit exists on remote**:
   ```bash
   git fetch origin
   git branch -r --contains HEAD
   ```
   If no remote branch contains HEAD, abort with an error asking the user to push their commits first.

9. **Create and push the tag**:
   ```bash
   git tag -a <new_version> -m "Release <new_version>"
   git push origin <new_version>
   ```

10. **Report success** with the new tag name. Then remind the user of the downstream pipeline:
    - `clickgres-pgext-packaging`'s hourly cron will auto-open `chore(pg_stat_ch): upgrade to <new_version>`. Merging it triggers `deb.yml` → S3 APT repo upload (~30–60 min) → GitHub release on the packaging repo signals the .debs are live.
    - To skip the hourly wait, manually dispatch `update-pg-stat-ch.yml` in `clickgres-pgext-packaging` immediately after the release tag is published.
    - After the .debs are in S3: trigger `postgres-vm-images / trigger-build.yml` to bake them into a new AMI (~55–60 min). Confirm the AMI build's `apt-get download` log line shows `<distversion>+<new_version>` for `postgresql-{16,17,18}-pg-stat-ch`.
    - After the AMI build: trigger `clickgres-platform / clickgres-pg-amis-latest.yml` to fetch new AMI IDs and open the `automated/pg-ami-upgrade` helm PR. Merge that to roll the fleet.

## Version Parsing

Parse version from tag like `v1.2.3`:
- Extract numbers after `v` prefix
- Split by `.` to get major, minor, patch components
- Handle edge case: if no tags exist, start at `v0.1.0`

## Error Handling

- If current commit is not on remote: "Current commit is not pushed to remote. Please push your changes first."
- If tag already exists: "Tag already exists. Please check existing tags."
- If git operations fail: Report the specific error.
