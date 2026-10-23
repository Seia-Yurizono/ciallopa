# Container version cleanup

The [ghcr-cleanup workflow](../.github/workflows/ghcr-cleanup.yml) runs after a successful
release tag build, every Monday at 03:23 Singapore time, and on manual dispatch.
It deletes GHCR package versions only, not Git tags, GitHub Releases, or Actions artifacts.

## Retention

Keep the newest **two tag names**, ranked by the creation time of their package version.
GHCR does not expose a separate timestamp for each tag. For tags on the same version,
`latest` sorts first, followed by alphabetical order. Thus `latest` and `0.1.0` count as
two tags even when they point to the same image; the older `0.1` version is deleted.
There is no minimum age for deletion, including for untagged versions.

Retain every manifest referenced by those images, recursively, including runnable platform
images, SBOM and provenance manifests. These may still appear as untagged in GitHub.
GitHub deletes package versions rather than individual tag aliases, so all aliases attached
to a retained digest survive. No cleanup is attempted if no tagged image exists or a retained
manifest cannot be read. The current release Docker job and cleanup share a concurrency group,
so those workflow versions cannot modify the package concurrently. The published `v0.1.0`
commit (`56ece6c`) predates that lock: rerunning its old release workflow does not inherit
the current protection. Before such a rebuild, pause cleanup triggers and wait for any running
cleanup to finish; avoid manual cleanup until publication is done, then restore the triggers.
External publishers must also coordinate with cleanup. Its snapshot check before deletion
does not make an uncoordinated upload atomic.

This keeps two labels, not necessarily two rollback builds. A deployment pinned to a deleted
tag or digest will no longer be able to pull it. Retention applies even to older release tags.
For rollback, keep the previous image locally and save an offline image archive alongside
its data backup before upgrading; follow [backup and restore](DEPLOYMENT.md#backup-and-restore).

## Run and inspect

Open [Actions → ghcr-cleanup](https://github.com/Seia-Yurizono/ciallopa/actions/workflows/ghcr-cleanup.yml),
select **Run workflow**, and use branch `main`.

- Leave **Preview only** checked to inspect the selected tags and deletion list in the run summary.
- Uncheck it to delete the listed versions immediately. Scheduled and release-triggered runs delete automatically.

The workflow first uses `GHCR_CLEANUP_TOKEN` if configured, otherwise its built-in `GITHUB_TOKEN`.
The built-in token needs this repository to have **Admin** access under the package's
**Package settings → Manage Actions access**. Publishing from Actions normally grants this
access automatically. Repository write access and package Admin access are distinct.

## Optional personal token

Use the package owner's account, or another account with Admin permission on this package.

1. Open [Create a personal access token (classic)](https://github.com/settings/tokens/new?scopes=read:packages,delete:packages&description=ciallopa-ghcr-cleanup).
2. Set an expiration date and select **read:packages** and **delete:packages**. Neither
   **repo** nor **write:packages** is required by the cleanup script. Use a classic token;
   GitHub Packages does not support fine-grained PAT authentication for this operation.
3. Click **Generate token** and copy it once.
4. Open [Repository Actions secrets](https://github.com/Seia-Yurizono/ciallopa/settings/secrets/actions)
   → **New repository secret**. Name it exactly `GHCR_CLEANUP_TOKEN`, paste the token in
   **Secret**, then save. If this page is unavailable, a repository administrator must add it.
5. Run the workflow with **Preview only** checked. After a successful preview, run it again
   with that option unchecked. Replace the secret when the token expires.

Enter the token directly in GitHub's secret form; do not put it in source files or chat.
A token cannot grant rights its owner does not already have on the package.

References: [GitHub package deletion and permissions](https://docs.github.com/en/packages/learn-github-packages/deleting-and-restoring-a-package),
[Container registry authentication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).
