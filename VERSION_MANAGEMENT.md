# Version Management Guide

## Semantic Versioning

This project follows [Semantic Versioning 2.0.0](https://semver.org/).

### Version Format: MAJOR.MINOR.PATCH

- **MAJOR**: Breaking changes (incompatible API changes)
- **MINOR**: New features (backward-compatible)
- **PATCH**: Bug fixes (backward-compatible)

### Examples

- `1.0.0` → `1.0.1`: Bug fix
- `1.0.1` → `1.1.0`: New feature added
- `1.1.0` → `2.0.0`: Breaking change

---

## How to Update Version

### 1. Update Version Number

Edit `__version__.py`:

```python
__version__ = "1.1.0"
__version_info__ = (1, 1, 0)
```

### 2. Update Changelog

Add entry to `CHANGELOG.md`:

```markdown
## [1.1.0] - 2025-12-30

### Added
- New export to CSV feature
- Custom date range selector

### Fixed
- Bug in trend calculation for edge cases
```

### 3. Commit Changes

```bash
git add Dashboards/dashboard/__version__.py
git add Dashboards/dashboard/CHANGELOG.md
git commit -m "Bump version to 1.1.0"
```

### 4. Create Git Tag

```bash
git tag -a v1.1.0 -m "Release version 1.1.0"
```

### 5. Push Changes

```bash
git push origin master
git push origin v1.1.0
```

---

## Feature Flags

Use feature flags in `__version__.py` for gradual rollouts:

```python
FEATURES = {
    "trend_analysis": True,
    "export_data": True,  # New feature
    "custom_metrics": False,  # Not ready yet
}
```

Check features in code:

```python
from __version__ import check_feature

if check_feature("export_data"):
    # Enable export button
    pass
```

---

## Version Checking in Code

```python
# Get version string
from dashboard import __version__
print(f"Dashboard version: {__version__}")

# Get version tuple for comparison
from dashboard import __version_info__
if __version_info__ >= (1, 1, 0):
    # Use new API
    pass
else:
    # Use legacy API
    pass

# Get full version with build info
from dashboard import get_full_version
print(f"Full version: {get_full_version()}")
```

---

## Release Checklist

Before releasing a new version:

- [ ] Update `__version__.py` with new version number
- [ ] Update `__version_info__` tuple
- [ ] Add entry to `CHANGELOG.md` with all changes
- [ ] Update feature flags if needed
- [ ] Run all tests
- [ ] Update documentation if API changed
- [ ] Review breaking changes (if MAJOR bump)
- [ ] Commit changes with descriptive message
- [ ] Create annotated git tag
- [ ] Push to remote repository
- [ ] Create GitHub release with changelog
- [ ] Notify team of new release

---

## Breaking Changes

When introducing breaking changes (MAJOR version bump):

1. Document all breaking changes clearly in CHANGELOG
2. Provide migration guide in documentation
3. Consider deprecation warnings in previous version
4. Update all affected documentation
5. Bump MAJOR version number

Example:

```markdown
## [2.0.0] - 2025-01-15

### Breaking Changes
- Removed deprecated `get_data()` method
- Changed configuration format (see migration guide)
- Required Python 3.10+ (was 3.8+)

### Migration Guide
1. Replace `get_data()` with `fetch_analytics_data()`
2. Update config.py format (see example in docs)
3. Upgrade Python to 3.10 or higher
```

---

## CI/CD Integration

For automated version management:

```bash
# Set build number (in CI/CD pipeline)
export BUILD_NUMBER="1234"

# Update __version__.py programmatically
python scripts/update_version.py --build $BUILD_NUMBER
```

In `__version__.py`:

```python
__build__ = "1234"  # Set by CI/CD

def get_full_version() -> str:
    version = __version__
    if __build__:
        version += f"+{__build__}"
    return version  # Returns "1.0.0+1234"
```

---

## Best Practices

1. **Never skip versions**: Go from 1.0.0 → 1.0.1, not 1.0.0 → 1.0.5
2. **Document everything**: Every version needs changelog entry
3. **Test before release**: Run full test suite
4. **Tag properly**: Always use annotated tags (`-a` flag)
5. **Communicate**: Notify team of breaking changes
6. **Keep it simple**: Don't overcomplicate versioning
7. **Be consistent**: Follow the same process every time
