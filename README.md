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

You need to do a minimal modification to libhdfs code when compiling it on OS X:

- change `<error.h>` to `<err.h>` in `hdfsJniHelper.c`
- change `md5sum` to `md5` in `src/saveVersion.sh`
- run `ant -Dcompile.c++=true -Dlibhdfs=true compile-c++-libhdfs` to build libhdfs.
- it is ok the build ends up with installation errors if you can already find compiled libs in `build/c++-build/Mac_OS_X-x86_64-64/libhdfs/.libs` or so

## Prepare ##

1. set `CLASSPATH`:

        for jr in `ls ../lib/*.jar`;do
        CLASSPATH=$jr:$CLASSPATH;done
        export CLASSPATH

    where `../lib` contains all the `.jar` essential to hdfs.

2. set `LD_LIBRARY_PATH` for Linux:

        export LD_LIBRARY_PATH=./lib:/opt/jdk/jre/lib/amd64/server

    make sure `libhdfs.so` and `libjvm` are declared in `LD_LIBRARY_PATH`

    You don't have to do this on OS X. You can always use `install_name_tool` to set or change a library's _install name_, also jvm on OS X is a _system framework_, so that it is not necessory to add jvm's path, while the only thing in step 3 is providing `hdfs.h` header path for _#cgo_.

3. correct the _#cgo_ header in `hdfs.go`, according to your enviornment.

## Test ##

After the preparation, run `go test`; or `go test -c` and `./hdfs.test`.

# Known Issues #

1. <del>Currently connecting to local file system is not handled correctly. So `Connect("", 0)` would lead to error.</del> It is okay now to access to local file system.
