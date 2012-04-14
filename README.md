# Package #

Go bindings for libhdfs, for manipulating files on Hadoop distributed file system.

# Types #

- `hdfs.Fs`: file system handle
- `hdfs.File`: file handle
- `hdfs.FileInfo`: file metadata structure, represented within Go

# Methods #

see `go doc`

# Usage #

## Prerequisite ##

- JVM
- HDFS: c bindings for libhdfs, java binary packages
- HDFS: configured cluster

## Prepare ##

1. set `CLASSPATH`:

        for jr in `ls ../lib/*.jar`;do
        CLASSPATH=$jr:$CLASSPATH;done
        export CLASSPATH

    where `../lib` contains all the `.jar` essential to hdfs.

2. set `LD_LIBRARY_PATH`:

        export LD_LIBRARY_PATH=./lib:/opt/jdk/jre/lib/amd64/server

    make sure `libhdfs.so` and `libjvm` are declared in `LD_LIBRARY_PATH`

3. correct the _cgo_ header in `hdfs.go`, according to your enviornment.


