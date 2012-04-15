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

### Tips for building libhdfs on OS X ###

- change `<error.h>` to `<err.h>` in `hdfsJniHelper.c`
- change `md5sum` to `md5` in `src/saveVersion.sh`
- run `ant -Dcompile.c++=true -Dlibhdfs=true compile-c++-libhdfs` to build libhdfs.
- it is ok the build ends up with installation errors if you can already find compiled libs in `build/c++-build/Mac_OS_X-x86_64-64/libhdfs/.libs` or so

### Tips for building libhadoop on OS X ###

Based on Hadoop-1.0.1; libhadoop would be loaded by `util.NativeCodeLoader` when accessing local file system.

1. java: change `-ljvm` to `-framework JavaVM` in both `Makefile.am` and `Makefile.in`
2. libz: apply [patch](https://issues.apache.org/jira/secure/attachment/12423498/HADOOP-3659.patch) to `acinclude.m4`:

        elif test ! -z "`which otool | grep -v 'no otool'`"; then
            ac_cv_libname_$1=\"`otool -L conftest | grep $1 | sed -e 's/^[  ]*//' -e 's/ .*//' -e 's/.*\/\(.*\)$/\1/'`\";

    and `configure`:

        elif test ! -z "`which otool | grep -v 'no otool'`"; then
          ac_cv_libname_z=\"`otool -L conftest | grep z | sed -e 's/^  *//' -e 's/ .*//' -e 's/.*\/\(.*\)$/\1/'`\";


3. apply [patch](https://gist.github.com/1327040) to source code `src/org/apache/hadoop/security/JniBasedUnixGroupsNetgroupMapping.c`.

4. run `ant compile-native`
5. put the compiled library `libhadoop.1.0.0.dylib` and its symbolic links in `/usr/lib/java`, which is one of the default element of `java.library.path`.

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

- After the preparation, correct the _constants_ in `hdfs_test.go`.
- run `go test`; or `go test -c` and `./hdfs.test`.

# Known Issues #

1. <del>Currently connecting to local file system is not handled correctly. So `Connect("", 0)` would lead to error.</del> It is okay now to access to local file system.
