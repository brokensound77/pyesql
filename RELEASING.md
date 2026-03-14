# Releasing pyesql

Releases are published to [PyPI](https://pypi.org/project/esql-py/) automatically via GitHub Actions
when a version tag is pushed. The workflow builds the package, publishes to TestPyPI, then waits
for manual approval before publishing to the real PyPI.

## Prerequisites

- Write access to the repo
- You are a required reviewer on the `pypi` GitHub environment

## Steps

### 1. Bump the version

Update the version in **both** places (they are verified to match before the tag is accepted):

- `pyproject.toml` → `version = "x.y.z"`
- `pyesql/__init__.py` → `__version__ = "x.y.z"`

Commit and push to main (directly or via a merged PR):

```bash
git commit -am "bump to vx.y.z"
git push
```

### 2. Tag and release

```bash
make release VERSION=x.y.z
```

This will:
1. Verify both version files match `x.y.z`
2. Create a git tag `vx.y.z`
3. Push the tag to GitHub, triggering the publish workflow

### 3. Approve the PyPI deployment

The workflow runs automatically in four stages:

| Stage | Trigger |
|---|---|
| `verify` | Confirms tag matches `pyproject.toml` version |
| `build` | Builds sdist and wheel |
| `publish-testpypi` | Uploads to [test.pypi.org](https://test.pypi.org/project/esql-py/) |
| `publish-pypi` | Waits for approval, then uploads to [pypi.org](https://pypi.org/project/esql-py/) |

Once `publish-testpypi` completes:

1. Go to **Actions → Publish → the running workflow**
2. Click **Review deployments**
3. Optionally verify the release looks correct on [test.pypi.org](https://test.pypi.org/project/esql-py/)
4. Click **Approve and deploy**

### 4. Verify

```bash
pip install --upgrade esql-py
python -c "import pyesql; print(pyesql.__version__)"
```

## Versioning

This project follows [Semantic Versioning](https://semver.org/):

- **Patch** `x.y.Z` — bug fixes, no API changes
- **Minor** `x.Y.0` — new features, backwards compatible
- **Major** `X.0.0` — breaking API changes
