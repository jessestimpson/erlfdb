/usr/local/libexec/fdbserver \
    -i JMS \
    -p 127.0.0.1:55400 \
    -C "/Users/jstimpson/dev/JesseStimpson.couchdb-erlfdb/.erlfdb/erlfdb.cluster" \
    -d "/Users/jstimpson/dev/JesseStimpson.couchdb-erlfdb/.erlfdb" \
    -L "/Users/jstimpson/dev/JesseStimpson.couchdb-erlfdb/.erlfdb" \
    --knob-disable_posix_kernel_aio=1
