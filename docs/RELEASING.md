# Releasing

1. Set the workspace version in `Cargo.toml` (three parts, e.g. `0.1.0`) and update
   `Cargo.lock`; describe the release in `docs/RELEASE_NOTES.md`.
2. Optionally run `.github/workflows/release.yml` by hand (`workflow_dispatch`) as a dry run: it
   runs every check, builds and smoke-tests the `.deb`, the tarball and the container image, and
   keeps the packages as workflow artifacts without pushing or releasing anything.
3. Push the tag `v<version>`. The workflow checks that the tag matches the crate version, pushes
   `ghcr.io/seia-yurizono/ciallopa` with the tags `<version>`, `<major>.<minor>` and `latest`, and
   creates the GitHub Release.

The workflow pushes to GHCR with the job's `GITHUB_TOKEN` (`packages: write`); no secret is
needed. A package created by the first push is private: make it public once under the
repository's Packages → `ciallopa` → Package settings → Change visibility. Later pushes keep the
visibility. The image's `org.opencontainers.image.source` label links the package to this
repository.
