# Binding Tester | erlfdb

## Updating FoundationDB Version

1. Checkout foundationdb at the same tag as what's supported by the GitHub action with `bindingtester: true`
2. `git apply /path/to/erlfdb/test/bindingtester/foundationdb.patch` and `git stash`
3. `git pull` and checkout the desired new tag
4. `git stash pop` and inspect the changes
5. `git diff > /path/to/erlfdb/test/bindingtester/foundationdb.patch`
6. Update the ci.yml GitHub action with the new target FoundationDB version.
