Developing in erlfdb
====================

.clangd config
--------------

The .clangd file cannot expand environment variables, so you should generate it
with the provided script, which assumes you're using asdf.

```bash
./dev/gen-clangd-config.sh > .clangd
```
