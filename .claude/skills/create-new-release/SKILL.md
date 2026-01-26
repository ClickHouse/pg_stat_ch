---
name: create-new-release
description: Create and push a new semver release tag. Asks for release type (patch/minor/major), validates commit exists on remote, bumps version, and pushes the tag.
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
   git tag -l 'v*' | sort -V | tail -1
   ```

4. **Parse and bump version** based on user selection:
   - **patch**: `v1.2.3` → `v1.2.4`
   - **minor**: `v1.2.3` → `v1.3.0`
   - **major**: `v1.2.3` → `v2.0.0`

5. **Validate current commit exists on remote**:
   ```bash
   git fetch origin
   git branch -r --contains HEAD
   ```
   If no remote branch contains HEAD, abort with an error asking the user to push their commits first.

6. **Create and push the tag**:
   ```bash
   git tag <new_version>
   git push origin <new_version>
   ```

7. **Report success** with the new tag name.

## Version Parsing

Parse version from tag like `v1.2.3`:
- Extract numbers after `v` prefix
- Split by `.` to get major, minor, patch components
- Handle edge case: if no tags exist, start at `v0.1.0`

## Error Handling

- If current commit is not on remote: "Current commit is not pushed to remote. Please push your changes first."
- If tag already exists: "Tag already exists. Please check existing tags."
- If git operations fail: Report the specific error.
