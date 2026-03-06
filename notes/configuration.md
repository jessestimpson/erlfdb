# Configuration

erlfdb supports compile-time and runtime configuration. Compile-time options are resolved in `rebar.config.script` during the build. Runtime options are set via application environment variables, typically in a `sys.config` file.

## Compile-Time Configuration

The following OS environment variables are read by `rebar.config.script` via `os:getenv/1` during compilation. These must be environment variables (rather than Erlang compile-time defines) because the script runs before Erlang compilation begins — it is responsible for producing the `erl_opts` and `port_env` that drive the build. The purpose of these options is to provide the developer with control over which libfdb_c function calls exist in the NIF library's symbol table, so that the intended library can be loaded at runtime.

### `ERLFDB_INCLUDE_DIR`

Path to the directory containing the FoundationDB C API header files. Defaults to `/usr/local/include`.

```shell
export ERLFDB_INCLUDE_DIR=/opt/foundationdb/include
```

### `ERLFDB_COMPILE_API_VERSION`

When set, the build uses this value directly as the compile-time FDB API version, bypassing `fdbcli` checks. This is useful in environments where `fdbcli` is not installed on the build host but the FoundationDB client library and headers are available (e.g. multi-version support, cross-compilation or CI). erlfdb will compile only with features that are supported in this version, and will throw a `badarg` error when calling code executes an erlfdb function that is not supported. For example, calling `erlfdb:get_mapped_range/4` when compiled with version 710.

```shell
export ERLFDB_COMPILE_API_VERSION=730
```

### `ERLFDB_FDBCLI`

Path to the `fdbcli` executable. If not set, erlfdb searches `PATH` and `/usr/local/bin`. The build script uses `fdbcli --version` to detect the FDB protocol version, which determines the C API version used during compilation. Ignored when `ERLFDB_COMPILE_API_VERSION` is set.

```shell
export ERLFDB_FDBCLI=/opt/foundationdb/bin/fdbcli
```

### `ERLFDB_ASSERT_API_VERSION`

When set to `"0"`, the build will continue with a warning if the FDB API version cannot be determined, instead of aborting. By default (any other value, or unset), an undetectable API version is a fatal error.

```shell
# Allow the build to proceed without a detected API version
export ERLFDB_ASSERT_API_VERSION=0
```

## Runtime Configuration

Runtime configuration is set via the `erlfdb` application environment, typically in a `sys.config` file or equivalent.

### `api_version`

The FoundationDB C API version to use at runtime. Defaults to the value of the `erlfdb_compile_time_api_version` compile-time macro, which is auto-detected from `fdbcli` during the build, or from `ERLFDB_COMPILE_API_VERSION`.

```erlang
[
    {erlfdb, [
        {api_version, 730}
    ]}
].
```

### `network_options`

A proplist of options passed to the FoundationDB client network layer during NIF initialization. Your options are merged with erlfdb's defaults, with your values taking precedence. Setting any option to `false` causes it to be removed from the resolved list, making it a no-op when options are applied to FDB. This can be used to disable a default.

The resolved options are stored in the `network_options_resolved` application env var and can be inspected at runtime:

```erlang
application:get_env(erlfdb, network_options_resolved).
```

#### Defaults

```erlang
[
    {callbacks_on_external_threads, true},
    {external_client_library, {erlfdb_network_options, compile_time_external_client_library, []}},
    {client_threads_per_version, 1}
].
```

#### Key Options

##### `callbacks_on_external_threads`

When `true`, allows libfdb_c to execute future callbacks on external threads. This increases throughput of future resolution and is recommended for erlfdb. Default: `true`.

##### `external_client_library`

Path (as a binary string) to the `libfdb_c` dynamic library. libfdb_c copies this library N times to temporary files so they can be individually loaded by external client threads.

The value may also be an `{M, F, A}` tuple. The function is called when the NIF is loaded and must return a binary string. Default: `{erlfdb_network_options, compile_time_external_client_library, []}`, which returns the library path detected at compile time.

##### `client_threads_per_version`

The number of client threads to create per dynamic library. This is the primary knob for scaling FDB client throughput horizontally.

- When set to `1`, only the local client thread is used.
- When set to `N > 1`, N external client threads are created in addition to the main thread.

The value may also be an `{M, F, A}` tuple. The function is called when the NIF is loaded and must return a positive integer.

> #### Note {: .info}
> For each thread, libfdb_c creates a TCP connection for each coordinator in the cluster file. Choose a value sufficient for your workload but no larger.

Default: `1`.

Example scaling to 2 threads:

```erlang
[
    {erlfdb, [
        {network_options, [
            {client_threads_per_version, 2}
        ]}
    ]}
].
```

##### `external_client_directory`

Path (as a binary string) to a directory containing additional `libfdb_c` dynamic libraries for multi-version client support. When set, libfdb_c loads all client libraries found in this directory, enabling connections to clusters running different FoundationDB versions. Each library version is loaded alongside the primary client.

```erlang
[
    {erlfdb, [
        {network_options, [
            {external_client_directory, <<"/usr/lib/foundationdb/multiversion">>}
        ]}
    ]}
].
```

##### `trace_enable`

A binary string path to a directory where client trace files will be written. The directory must exist and be writable.

##### `trace_format`

The format of trace files. Supported values: `<<"xml">>` (default) and `<<"json">>`.

```erlang
[
    {erlfdb, [
        {network_options, [
            {trace_enable, <<"/var/log/fdb_traces">>},
            {trace_format, <<"json">>}
        ]}
    ]}
].
```

#### All Supported Network Options

The full set of network options corresponds to the [FoundationDB C API network options](https://apple.github.io/foundationdb/api-c.html#c.fdb_network_set_option).

Any option value may be set to `false` to disable it, even if it was set in the defaults. When `false`, the option key and value do not appear in the resolved options.

Any option value may be set to `{M, F, A}` tuple to acquire the value at runtime. The option key and value will appear in the resolved options.
