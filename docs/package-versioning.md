# Package Versioning

This repository uses tag-based package versioning with `MinVer`.

## How package versions are determined

Package version is derived from the Git tag history.

- Stable release tag: `v1.2.3`
- Alpha prerelease tag: `v1.2.3-alpha.1`
- Beta prerelease tag: `v1.2.3-beta.1`
- RC prerelease tag: `v1.2.3-rc.1`

When a package build runs from one of those tags, the produced NuGet packages use the matching version without the leading `v`.

Examples:

- `v0.1.0` -> `0.1.0`
- `v0.1.0-alpha.1` -> `0.1.0-alpha.1`
- `v0.1.0-beta.2` -> `0.1.0-beta.2`

## Workflows

### CI package artifacts

The workflow [package.yml](../.github/workflows/package.yml) runs on:

- pull requests
- pushes to `master`
- manual dispatch

It creates `.nupkg` and `.snupkg` files as GitHub Actions artifacts.

These packages are for validation, download, and inspection only.
They are not published to NuGet and remain attached to the workflow run in GitHub Actions.

### Release packages

The workflow [release.yml](../.github/workflows/release.yml) can run in two ways:

- automatically when a tag matching `v*` is pushed
- manually through GitHub Actions `workflow_dispatch` from `master` by entering a version such as `0.2.0` or `0.2.0-beta.1`

Those packages are the release packages and are the ones intended to be published to NuGet.

## Typical release flow

### Option 1: Run The Release Workflow Manually

1. Merge the desired changes to `master`.
2. Open the `Release Packages` workflow in GitHub Actions on the `master` branch.
3. Run the workflow and enter the target version without the leading `v`.
4. The workflow creates and pushes the matching tag.
5. The workflow builds the versioned packages.
6. Publish those packages to NuGet.

### Option 2: Push A Tag Yourself

1. Merge the desired changes to `master`.
2. Create a tag for the target version.
3. Push the tag.
4. Let `release.yml` build the versioned packages.
5. Publish those packages to NuGet.

Example:

```bash
git tag v0.2.0-beta.1
git push origin v0.2.0-beta.1
```

## Notes

- All library packages in `src/` share the same version.
- If a build runs without a release tag, `MinVer` can still infer a prerelease version for CI artifacts.
- CI artifact versions are acceptable for internal validation, but tagged builds should be used for public package publishing.
