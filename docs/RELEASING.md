# Releasing

1. Set the workspace version in `Cargo.toml` (e.g. `0.1.1`) and update `Cargo.lock`.
   Update the download and raw-file URLs in `README.md`, the exact image tag in
   `packaging/docker/compose.yaml`, and `docs/RELEASE_NOTES.md` together. Check that the
   installation examples refer to the version being released: registry cleanup can remove
   the previous image as soon as the new release succeeds.
2. Optionally run `.github/workflows/release.yml` by hand (`workflow_dispatch`) as a dry run: it
   runs every check, builds and smoke-tests the `.deb`, the tarball and the container image, and
   keeps the packages as workflow artifacts without pushing or releasing anything.
3. Publish from a commit containing the current workflow. Push the tag `v<version>`; checks
   require it to match the crate version. The image at `ghcr.io/seia-yurizono/ciallopa` receives
   `<version>` and `latest`. Only stable releases also move `<major>.<minor>`. Prereleases
   include versions with a hyphen and the explicitly rebuilt `v0.1.0`; their GitHub Release
   remains Pre-release and is not marked as GitHub's Latest release. This is separate from
   the container's `latest` alias.

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
