#!/usr/bin/env python3
"""Keep the two newest GHCR tags and their manifest closure; delete other versions
(except untagged uploads younger than GRACE)."""
import argparse
import base64
import datetime
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


class Client:
    def __init__(self, owner, package, owner_type, token):
        self.token = token
        self.repo = f"{owner.lower()}/{package}"
        namespace = 'orgs' if owner_type == 'Organization' else 'users'
        self.path = f"/{namespace}/{owner}/packages/container/{urllib.parse.quote(package, safe='')}/versions"
        self.registry_token = None

    @staticmethod
    def request(url, headers, method='GET'):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers, method=method), timeout=30) as r:
                raw = r.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            # Never print request headers or credentials.
            raise RuntimeError(f"{method} {urllib.parse.urlsplit(url).path}: HTTP {e.code}") from None

    def api(self, path, method='GET'):
        return self.request('https://api.github.com' + path, {
            'Authorization': 'Bearer ' + self.token,
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        }, method)

    def versions(self):
        result = []
        page = 1
        while True:
            batch = self.api(f'{self.path}?per_page=100&page={page}')
            result.extend(batch)
            if len(batch) < 100:
                return result
            page += 1

    def manifest(self, digest):
        if self.registry_token is None:
            query = urllib.parse.urlencode({'service': 'ghcr.io', 'scope': f'repository:{self.repo}:pull'})
            login = os.environ.get('GITHUB_ACTOR', 'ciallopa-cleanup')
            auth = base64.b64encode(f'{login}:{self.token}'.encode()).decode()
            self.registry_token = self.request('https://ghcr.io/token?' + query, {
                'Authorization': 'Basic ' + auth,
            })['token']
        return self.request(f'https://ghcr.io/v2/{self.repo}/manifests/{digest}', {
            'Authorization': 'Bearer ' + self.registry_token,
            'Accept': ', '.join([
                'application/vnd.oci.image.index.v1+json',
                'application/vnd.oci.image.manifest.v1+json',
                'application/vnd.docker.distribution.manifest.list.v2+json',
                'application/vnd.docker.distribution.manifest.v2+json',
            ]),
        })


def tags(version):
    return version['metadata']['container']['tags']


# The release pushes each architecture's image by digest before one job tags the merged
# multi-arch index; never delete such a recent untagged upload while a release may be running.
GRACE = datetime.timedelta(hours=6)


def created(version):
    return datetime.datetime.fromisoformat(version['created_at'].replace('Z', '+00:00'))


def plan(versions, manifest, now=None):
    # GHCR has no per-tag timestamp. Use the version creation date; moving an alias
    # does not make an old image a new build. Deterministic ordering handles ties.
    ordered = sorted(versions, key=lambda v: (v['created_at'], v['id']), reverse=True)
    selected = []
    for version in ordered:
        for tag in sorted(tags(version), key=lambda t: (t != 'latest', t)):
            selected.append((tag, version['name']))
            if len(selected) == 2:
                break
        if len(selected) == 2:
            break
    if not selected:
        raise RuntimeError('No tagged image found; refusing to delete the entire package.')
    protected = set()

    def visit(digest):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
            raise RuntimeError('Invalid manifest digest; refusing cleanup.')
        if digest in protected:
            return
        protected.add(digest)
        doc = manifest(digest)  # Resolve every retained manifest before any DELETE.
        if 'manifests' in doc:
            for child in doc['manifests']:
                visit(child['digest'])
        elif not ('config' in doc and 'layers' in doc):
            raise RuntimeError(f'Unrecognized manifest: {digest}')

    for _, digest in selected:
        visit(digest)
    # Delete parent indexes before their unreferenced child manifests where possible.
    # Package versions are manifests, not individual filesystem layers.
    now = now or datetime.datetime.now(datetime.timezone.utc)
    delete = [v for v in ordered if v['name'] not in protected and now - created(v) >= GRACE]
    return selected, protected, delete


def snapshot(versions):
    return sorted((v['id'], v['name'], tuple(sorted(tags(v)))) for v in versions)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Delete planned versions (default: preview only)')
    args = parser.parse_args()
    owner, package = os.environ['GITHUB_REPOSITORY'].split('/', 1)
    client = Client(owner, package.lower(), os.environ.get('PACKAGE_OWNER_TYPE', 'User'), os.environ['GH_TOKEN'])
    versions = client.versions()
    selected, protected, delete = plan(versions, client.manifest)
    lines = ['# GHCR cleanup', '', f"Mode: {'DELETE' if args.apply else 'preview'}", '',
             'Selected tags: ' + ', '.join(f'`{tag}`' for tag, _ in selected),
             f'Protected manifests: {len(protected)}; versions to delete: {len(delete)}.', '',
             'All aliases on a protected digest are retained (GitHub deletes versions, not individual tags).',
             f'Untagged versions newer than {GRACE} are kept for a running release.', '']
    for v in delete:
        lines.append(f"- Delete version {v['id']}: `{v['name']}`; tags: {', '.join(tags(v)) or '(untagged)'}")
    report = '\n'.join(lines) + '\n'
    print(report, flush=True)
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
            f.write(report)
    if args.apply:
        if snapshot(client.versions()) != snapshot(versions):
            raise RuntimeError('Package changed during planning; rerun cleanup against a fresh snapshot.')
        for v in delete:
            client.api(f"{client.path}/{v['id']}", 'DELETE')
            print(f"Deleted version {v['id']}", flush=True)
        # A successful run must leave the protected graph readable.
        for digest in protected:
            client.manifest(digest)
        print('Cleanup complete; retained manifests are readable.')


if __name__ == '__main__':
    main()
