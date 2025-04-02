# FDB Client Threads

The default configuration of erlfdb includes the creation and management of a single OS thread that is responsible for all database work created by processes in the Erlang VM. As your workload increases in size, you will need to configure the erlfdb client for horizontal scaling. Otherwise, a sufficiently sized system with a sufficiently sized workload can saturate the single client thread with work, which will lead to high latency on individual requests, and a ceiling on throughput.

This page will guide you through the configuration necessary to scale the FDB client to several worker threads on a single BEAM VM.

## Network options configuration

The configuration will be done via the erlfdb `network_options` environment variable. In erlfdb >= 0.3, defaults are chosen that are meant to ease the configuration burden. The options are described in detail below, but the main takeaway is the following:

> #### Tip {: .tip}
>
> In order to double your client's maximum throughput, set
> `client_threads_per_version` to 2.

```erlang
[
    {erlfdb, [
        {network_options, [
            {client_threads_per_version, 2}
        ]}
    ]}
].
```

Please read on for details about the scaling implementation.

### Version 0.3 and above

In versions 0.3 and above, the env var `network_options` defaults to

```erlang
[
    {erlfdb, [
        {network_options, [
            % `ignore_external_client_failures`:
            % Optional: Allows your client to prepare fdbserver version
            % upgrades. When a future client library fails to load, it's not a
            % failure condition, because we know it will be valid at some
            % point in the future.
            {ignore_external_client_failures, true},

            % `callbacks_on_external_threads`:
            % Allows libfdb_c to apply future callbacks on external threads,
            % which increases the throughput of future resolution. erlfdb
            % simply sends the result to the required Erlang process, so the
            % default behavior of executing on the main thread is not
            % desirable
            {callbacks_on_external_threads, true},

            % `external_client_directory`:
            % Points libfdb_c to the directory containing the dynamic
            % library(ies) to be loaded. libfdb_c will copy each .so N times
            % to temporary files on the filesystem so that they can be
            % individually loaded. Normally the value is a binary string of
            % the directory on the filesystem containing the dynamic
            % library(ies).
            %
            % You may also provide an `{M, F, A}` tuple. If you do, the function
            % is executed at the time the `erlfdb_nif` module is loaded. The
            % return must be a binary string. This value is used as the
            % permanent value for `external_client_directory` for the lifetime
            % of the VM.
            {external_client_directory, {erlfdb_network_options, compile_time_external_client_directory, []}},

            % `client_threads_per_version`:
            % Number of threads to create per dynamic library. This is the
            % value to tweak in order to scale the number of client threads
            % horizontally. This value is used as the permanent value
            % for the lifetime of the BEAM VM.
            %
            % When the value is 1, only the local client is used. When the
            % value is N > 1, then N external client threads are used.
            %
            % You may also provide an `{M, F, A}` tuple. If you do, the
            % function is executed at the time the `erlfdb_nif` module is
            % loaded. The return must be a positive integer.
            %
            % Note: For each thread, libfdb_c will create a tcp
            % connection for each coordinator in the cluster file.
            {client_threads_per_version, 1}
        ]}
    ]}
].
```

When the erlfdb NIF is loaded, any network_options defined by your config are merged into the defaults, so that yours always take precedence. The final set of resolved network_options is stored in env var `network_options_resolved`.

```erlang
application:get_env(erlfdb, network_options_resolved).
```

### How to choose the correct value for `client_threads_per_version`

The ideal approach is to choose an integer value that is sufficient for your workload, and no more. It's not recommended to choose a value that is larger than the number of online schedulers.

Instead of choosing a specific value, you may wish for erlfdb to scale dynamically along with your scaling of the Erlang VM itself. Consider the following MFA-tuple choices.

- `{erlang, system_info, [schedulers_online]}`
- `{erlang, system_info, [dirty_io_schedulers]}`

> ** Note **: These suggestions are made simply as a convenience for automatic scaling, and none of these choices will associate erlfdb external client threads with Erlang's schdeulers or dirty_io_schedulers. External client threads will always be created **in addition to** and **not replacements for** any of the Erlang VM's OS threads.

## Making use of External Client Threads

### Distributing your workfload

So you've configured the client to create multiple external client threads. Now, you need to make sure your application distributes the workload among these threads.

The function `erlfdb:open/2` provides your application with a convenient way to distribute your workload. Whenever you need a database object from which to create a transaction, call `open` to retrieve one very quickly (via persistent_term storage). With the default work distribution strategy (`scheduler_id`), erlfdb will create and keep track of N database objects where N is the number of schedulers on your system. Each of these database objects will automatically be assigned to one of the external client threads in libfdb_c.

> #### Info {: .info}
> This strategy does not claim that a particular item of work will remain on an Erlang scheduler -- only that the scheduler_id itself is helpful in distributing work in a roughly even fashion.

It may be beneficial to create all database objects during startup. The function `erlfdb:open_all/2` is provided for this purpose.

### Thread lifecycle

When `client_threads_per_version` > 1, the behavior of erlfdb is as follows:

1. `enif_thread_create` is called exactly once. This is still the 'local client network thread' a.k.a. the libfdb_c Main Thread. This thread is given the name `fdb:network_thread`.
2. libfdb_c creates N copies of the dynamic library into temp files on the filesystem. Each copy will house 1 of the external threads.
3. N threads are created by libfdb_c. These threads are *not* created with `enif_thread_create`, because the creation is contained in logic internal to libfdb_c. Each of these threads is given the name `fdb-<vsn>-<index>`. If the thread name is longer than 15 chars, it's instead given the name `fdb-<vsn>`. If this is longer than 15 chars, it's given the name `fdb-external`. On a Linux system, these threads are visible with `top -H -p $beam_pid`.
4. Each database object (via `erlfdb:create_database/1`) is linked to a client thread at the time of creation. The threads are distributed in a round-robin fashion. Therefore, to make use of N client threads, you must have N database objects.
5. Shutdown: Each external thread is waited upon immediately after the local client network event loop returns. Thus, you may consider the external client threads as "children" of the local client network thread created by `enif_thread_create`. This relationship is necessary and sufficient for the Erlang VM and its operator to maintain control over the OS threads on the system.

## Past Versions 0.0 - 0.2

In versions 0.0.x - 0.2.x, the env var `network_options` defaults to `[]`. The behavior of the erlfdb NIF with respect to thread creation is as follows:

`enif_thread_create` is called exactly once. This is the 'local client network thread', of which libfdb_c can only have one maximum. No other threads are started by libfdb_c. In other words, there is 1 event loop for the entire BEAM.

In these erlfdb versions, the `client_threads_per_version` env var is not supported, so horizontal scaling of a single client is not possible. Instead, consider starting multiple Erlang VMs in order to distribute your workload, or upgrade to erlfdb >= 0.3.

## References

A short list of suggested reading in the FoundationDB source that will help the reader understand the details of the client threading model.

1. `fdbclient/MultiVersionTransaction.actor.cpp`: implements the management of the local and external clients.
2. `bindings/c/fdb_c.cpp`: defines MultiVersionApi as the default implementation
2. `fdbclient/NativeAPI.actor.cpp`: implements the client (local or external). Specifically, maintains the reference to the thread.
3. `flow/Net2.actor.cpp`: implements the event loop
