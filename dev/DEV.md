# Developing in erlfdb

## C NIF - clang-format

This section describes minimal steps for working with clang-format, which is used to
apply consistent code formatting to the .c files. 

### Installation

Here's an example installation on a macOS system.

```bash
brew install clang-format
```

Often clang-format can be used inside an IDE.

### .clangd config

The .clangd file cannot expand environment variables, so you should generate it
with the provided script, which assumes you're using asdf.

```bash
./dev/gen-clangd-config.sh > .clangd
```

### Example run

To reformat a file in-place, execute the following.

```bash
clang-format -i c_src/main.c
```
