# Releasing

`workspace.package.version` in [Cargo.toml](../Cargo.toml) is the source of the application
version. Crates already inherit it; packaging and image publication read it through Cargo
metadata, the built binary or the matching Git tag. Provider versions and database schema
versions are independent and must not be bumped with the application version.

1. Set the workspace version in `Cargo.toml`, then run:

   ```sh
   cargo update --workspace
   python3 scripts/release-version.py
   ```

   Cargo regenerates workspace versions in `Cargo.lock`. The Python script (3.11+) updates
   pinned download URLs and package filenames in `README.md`, and the exact Compose image
   tag. Keep these generated changes in the version commit: Markdown and a standalone
   Compose file cannot read the repository's TOML at install time. CI runs the script with
   `--check` to reject stale references without changing files.

   Write the changes and release-specific migration notes in `docs/RELEASE_NOTES.md`.
   Historical versions in those notes are intentional; the script does not rewrite them.
   Shared installation and tag policy belongs in [deployment](DEPLOYMENT.md#image-tags-and-upgrades),
   rather than another copy of the current version in each document.
2. Optionally run `.github/workflows/release.yml` by hand (`workflow_dispatch`) as a dry run: it
   runs every check, builds and smoke-tests the `.deb`, the tarball and the container image, and
   keeps the packages as workflow artifacts without pushing or releasing anything.
3. Publish from a commit containing the current workflow. Push the tag `v<version>`; checks
   require it to match the crate version. Image aliases follow the
   [deployment policy](DEPLOYMENT.md#image-tags-and-upgrades). Prereleases include versions
   with a hyphen and the explicitly rebuilt `v0.1.0`; their GitHub Release remains Pre-release
   and is not marked as GitHub's Latest release. This is separate from the container's
   `latest` alias. The workflow supplies the release title from the tag; release notes do
   not need a second current-version heading.

The current Docker publication job and registry cleanup use the same concurrency group.
The already published `v0.1.0` commit (`56ece6c`) predates that lock: rerunning its old workflow
does not pick up the protection from `main`. If rebuilding that tag is necessary, pause cleanup
triggers, wait for any active cleanup to finish, and keep manual/external cleanup stopped until
publication finishes; restore normal cleanup afterwards. Prefer a new version from current
`main`. See [container cleanup](CONTAINER_CLEANUP.md) for the two-tag policy and
[deployment recovery](DEPLOYMENT.md#backup-and-restore) for saving a previous image offline.

The workflow pushes to GHCR with the job's `GITHUB_TOKEN` (`packages: write`); no secret is
needed. A package created by the first push is private: make it public once under the
repository's Packages → `ciallopa` → Package settings → Change visibility. Later pushes keep the
visibility. The image's `org.opencontainers.image.source` label links the package to this
repository.
