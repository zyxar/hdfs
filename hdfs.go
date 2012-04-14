package hdfs

// #cgo linux CFLAGS: -I/opt/jdk/include -I/opt/jdk/include/linux
// #cgo linux LDFLAGS: -Llib -lhdfs -L/opt/jdk/jre/lib/amd64/server -ljvm -Wl,-rpath -Wl,/opt/jdk/jre/lib/amd64/server -Wl,-rpath -Wl,/home/marcus/Desktop/go/hdfs/hdfs/lib
// #include "hdfs.h"
/*
int getlen(char*** ptr) {
    int i = 0;
    while (ptr[i] != NULL) ++i;
    return i;
}
int getl(char*** ptr) {
    int i = 0;
    while (ptr[0][i] != NULL) ++i;
    return i;
}
char* getstring(char*** ptr, int i, int j) {
    return ptr[i][j];
}
*/
import "C"

import (
    "unsafe"
    "time"
)

const (
    O_RDONLY  = int(C.O_RDONLY)
    O_WRONLY  = int(C.O_WRONLY)
    O_CREATE  = int(C.O_CREAT)
    O_APPEND  = int(C.O_APPEND)
    EINTERNAL = int(C.EINTERNAL)
)

type hdfsFS struct {
    cptr C.hdfsFS
}

type hdfsFile struct {
    cptr C.hdfsFile
}

type hdfsFileInfo struct {
    cptr C.hdfsFileInfo
}

type FileInfo struct {
    meta         hdfsFileInfo
    Kind         byte
    Name         string
    LastMod      time.Time
    Size         int64
    Replication  int16
    BlockSize    int64
    Owner, Group string
    Permissions  int16
    LastAccess   time.Time
}

type File hdfsFile
type Fs hdfsFS

/** 
 * ConnectAsUser - Connect to a hdfs file system as a specific user
 * Connect to the hdfs.
 * @param host A string containing either a host name, or an ip address
 * of the namenode of a hdfs cluster. 'host' should be passed as "" if
 * you want to connect to local filesystem. 'host' should be passed as
 * 'default' (and port as 0) to used the 'configured' filesystem
 * (core-site/core-default.xml).
 * @param port The port on which the server is listening.
 * @param user the user name (this is hadoop domain user). Or "" is equivelant to Connect(host, port)
 * @return Returns a handle to the filesystem or nil on error.
 */
// TODO: access local filesystem
func ConnectAsUser(host string, port uint16, user string) (*Fs, error) {
    var h *C.char
    var u *C.char
    if host == "" {
        h = (*C.char)(unsafe.Pointer(uintptr(0)))
    } else {
        h = C.CString(host)
        defer C.free(unsafe.Pointer(h))
    }
    if user == "" {
        u = (*C.char)(unsafe.Pointer(uintptr(0)))
    } else {
        u = C.CString(user)
        defer C.free(unsafe.Pointer(u))
    }

    ret, err := C.hdfsConnectAsUser(h, C.tPort(port), u)
    if err != nil {
        return nil, err
    }
    return &Fs{ret}, nil
}

/** 
 * Connect - Connect to a hdfs file system.
 * Connect to the hdfs.
 * @param host A string containing either a host name, or an ip address
 * of the namenode of a hdfs cluster. 'host' should be passed as "" if
 * you want to connect to local filesystem. 'host' should be passed as
 * 'default' (and port as 0) to used the 'configured' filesystem
 * (core-site/core-default.xml).
 * @param port The port on which the server is listening.
 * @return Returns a handle to the filesystem or nil on error.
 */
func Connect(host string, port uint16) (*Fs, error) {
    return ConnectAsUser(host, port, "")
}

/** 
 * Disconnect - Disconnect from the hdfs file system.
 * Disconnect from hdfs.
 * @param fs The configured filesystem handle.
 */
func (fs *Fs) Disconnect() error {
    _, err := C.hdfsDisconnect(fs.cptr)
    return err
}

func Disconnect(fs *Fs) error {
    _, err := C.hdfsDisconnect(fs.cptr)
    return err
}

/** 
 * OpenFile - Open a hdfs file in given mode.
 * @param fs The configured filesystem handle.
 * @param path The full path to the file.
 * @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT), 
 * O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
 * @param bufferSize Size of buffer for read/write - pass 0 if you want
 * to use the default configured values.
 * @param replication Block replication - pass 0 if you want to use
 * the default configured values.
 * @param blocksize Size of block - pass 0 if you want to use the
 * default configured values.
 * @return Returns the handle to the open file or nil on error.
 */
// work around: fix ESRCH for CREATE|WRONLY
func (fs *Fs) OpenFile(path string, flags int, buffersize int, replication int, blocksize uint32) (*File, error) {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    file, err := C.hdfsOpenFile(fs.cptr, p, C.int(flags), C.int(buffersize), C.short(replication), C.tSize(blocksize))
    if file == (C.hdfsFile)(unsafe.Pointer(uintptr(0))) {
        return nil, err
    }
    return &File{file}, nil
}

/** 
 * CloseFile - Close an open file. 
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns nil on success, or error.  
 */
func (fs *Fs) CloseFile(file *File) error {
    _, err := C.hdfsCloseFile(fs.cptr, file.cptr)
    return err
}

/** 
 * Exists - Checks if a given path exsits on the filesystem 
 * @param fs The configured filesystem handle.
 * @param path The path to look for
 * @return Returns nil on success, or error.
 */
func (fs *Fs) Exists(path string) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsExists(fs.cptr, p)
    return err
}

/** 
 * Seek - Seek to given offset in file. 
 * This works only for files opened in read-only mode. 
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param desiredPos Offset into the file to seek into.
 * @return Returns nil on success, or error.  
 */
func (fs *Fs) Seek(file *File, pos int64) error {
    _, err := C.hdfsSeek(fs.cptr, file.cptr, C.tOffset(pos))
    return err
}

/** 
 * Tell - Get the current offset in the file, in bytes.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Current offset, or error.
 */
func (fs *Fs) Tell(file *File) (int64, error) {
    ret, err := C.hdfsTell(fs.cptr, file.cptr)
    if err != nil {
        return -1, err
    }
    return int64(ret), nil
}

/** 
 * Read - Read data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return Returns the number of bytes actually read, possibly less
 * than than length; or error
 */
func (fs *Fs) Read(file *File, buffer []byte, length int) (uint32, error) {
    ret, err := C.hdfsRead(fs.cptr, file.cptr, (unsafe.Pointer(&buffer[0])), C.tSize(length))
    return uint32(ret), err
}

/** 
 * Pread - Positional read of data from an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param position Position from which to read
 * @param buffer The buffer to copy read bytes into.
 * @param length The length of the buffer.
 * @return Returns the number of bytes actually read, possibly less than
 * than length; or error
 */
func (fs *Fs) Pread(file *File, position int64, buffer []byte, length int) (uint32, error) {
    ret, err := C.hdfsPread(fs.cptr, file.cptr, C.tOffset(position), (unsafe.Pointer(&buffer[0])), C.tSize(length))
    return uint32(ret), err
}

/** 
 * Write - Write data into an open file.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @param buffer The data.
 * @param length The no. of bytes to write. 
 * @return Returns the number of bytes written; or error.
 */
// work around: fix ESRCH
func (fs *Fs) Write(file *File, buffer []byte, length int) (uint32, error) {
    ret, err := C.hdfsWrite(fs.cptr, file.cptr, (unsafe.Pointer(&buffer[0])), C.tSize(length))
    if ret == C.tSize(-1) {
        return 0, err
    }    
    return uint32(ret), nil
}

/** 
 * Flush - Flush the data. 
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) Flush(file *File) error {
    _, err := C.hdfsFlush(fs.cptr, file.cptr)
    return err
}

/**
 * Available - Number of bytes that can be read from this
 * input stream without blocking.
 * @param fs The configured filesystem handle.
 * @param file The file handle.
 * @return Returns available bytes; or error. 
 */
// work around: fix ESRCH
func (fs *Fs) Available(file *File) (uint32, error) {
    ret, err := C.hdfsAvailable(fs.cptr, file.cptr)
    if ret == C.int(-1) {
        return 0, err
    }
    return uint32(ret), nil
}

/**
 * Copy - Copy file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file. 
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) Copy(src string, dstFS *Fs, dst string) error {
    srcstr := C.CString(src)
    dststr := C.CString(dst)
    defer C.free(unsafe.Pointer(srcstr))
    defer C.free(unsafe.Pointer(dststr))
    ret, err := C.hdfsCopy(fs.cptr, srcstr, dstFS.cptr, dststr)
    if ret == C.int(-1) {
        return err
    }
    return nil
}

/**
 * Move - Move file from one filesystem to another.
 * @param srcFS The handle to source filesystem.
 * @param src The path of source file. 
 * @param dstFS The handle to destination filesystem.
 * @param dst The path of destination file. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) Move(src string, dstFS *Fs, dst string) error {
    srcstr := C.CString(src)
    dststr := C.CString(dst)
    defer C.free(unsafe.Pointer(srcstr))
    defer C.free(unsafe.Pointer(dststr))
    ret, err := C.hdfsMove(fs.cptr, srcstr, dstFS.cptr, dststr)
    if ret == C.int(-1) {
        return err
    }
    return nil
}

/**
 * Delete - Delete file. 
 * @param fs The configured filesystem handle.
 * @param path The path of the file. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) Delete(path string) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsDelete(fs.cptr, p)
    return err
}

/**
 * Rename - Rename file. 
 * @param fs The configured filesystem handle.
 * @param oldPath The path of the source file. 
 * @param newPath The path of the destination file. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) Rename(oldpath, newpath string) error {
    op, np := C.CString(oldpath), C.CString(newpath)
    defer C.free(unsafe.Pointer(op))
    defer C.free(unsafe.Pointer(np))
    _, err := C.hdfsRename(fs.cptr, op, np)
    return err
}

/** 
 * GetWorkingDirectory - Get the current working directory for
 * the given filesystem.
 * @param fs The configured filesystem handle.
 * @param buffer The user-buffer to copy path of cwd into. 
 * @param bufferSize The length of user-buffer.
 * @return Returns buffer, or error.
 */
func (fs *Fs) GetWorkingDirectory(buffer []byte, size uint32) ([]byte, error) {
    _, err := C.hdfsGetWorkingDirectory(fs.cptr, (*C.char)(unsafe.Pointer(&buffer[0])), C.size_t(size))
    if err != nil {
        return nil, err
    }
    return buffer, nil
}

/** 
 * SetWorkingDirectory - Set the working directory. All relative
 * paths will be resolved relative to it.
 * @param fs The configured filesystem handle.
 * @param path The path of the new 'cwd'. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) SetWorkingDirectory(path string) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsSetWorkingDirectory(fs.cptr, p)
    return err
}

/** 
 * CreateDirectory - Make the given file and all non-existent
 * parents into directories.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) CreateDirectory(path string) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsCreateDirectory(fs.cptr, p)
    return err
}

/** 
 * SetReplication - Set the replication of the specified
 * file to the supplied value
 * @param fs The configured filesystem handle.
 * @param path The path of the file. 
 * @return Returns nil on success, or error. 
 */
func (fs *Fs) SetReplication(path string, replication int16) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsSetReplication(fs.cptr, p, C.int16_t(replication))
    return err
}

/** 
 * ListDirectory - Get list of files/directories for a given
 * directory-path.
 * @param fs The configured filesystem handle.
 * @param path The path of the directory. 
 * @return Returns a slice of FileInfo struct pointer, or nil on error.
 */
func (fs *Fs) ListDirectory(path string) ([]*FileInfo, error) {
    var num int
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    info, err := C.hdfsListDirectory(fs.cptr, p, (*C.int)(unsafe.Pointer(&num)))
    if err != nil {
        return nil, err
    }
    defer C.hdfsFreeFileInfo(info, C.int(num))
    ret := make([]*FileInfo, num)
    var cinfo *C.hdfsFileInfo
    for i := 0; i < num; i++ {
        cinfo = (*C.hdfsFileInfo)(unsafe.Pointer(uintptr(unsafe.Pointer(info)) + uintptr(i)*unsafe.Sizeof(C.hdfsFileInfo{})))
        ret[i] = new(FileInfo)
        ret[i].meta = hdfsFileInfo{*cinfo}
        ret[i].Kind = byte(cinfo.mKind)
        ret[i].Name = C.GoString(cinfo.mName)
        ret[i].LastMod = time.Unix(int64(cinfo.mLastMod), int64(0))
        ret[i].Size = int64(cinfo.mSize)
        ret[i].Replication = int16(cinfo.mReplication)
        ret[i].BlockSize = int64(cinfo.mBlockSize)
        ret[i].Owner = C.GoString(cinfo.mOwner)
        ret[i].Group = C.GoString(cinfo.mGroup)
        ret[i].Permissions = int16(cinfo.mPermissions)
        ret[i].LastAccess = time.Unix(int64(cinfo.mLastAccess), int64(0))
    }
    return ret, nil
}

/** 
 * GetPathInfo - Get information about a path as a (dynamically
 * allocated) single FileInfo struct pointer.
 * @param fs The configured filesystem handle.
 * @param path The path of the file. 
 * @return Returns a pointer to FileInfo object, or nil on error.
 */
func (fs *Fs) GetPathInfo(path string) (*FileInfo, error) {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    info, err := C.hdfsGetPathInfo(fs.cptr, p)
    if info == nil {
        return nil, err
    }
    defer C.hdfsFreeFileInfo(info, C.int(1))
    ret := new(FileInfo)
    ret.meta = hdfsFileInfo{*info}
    ret.Kind = byte(info.mKind)
    ret.Name = C.GoString(info.mName)
    ret.LastMod = time.Unix(int64(info.mLastMod), int64(0))
    ret.Size = int64(info.mSize)
    ret.Replication = int16(info.mReplication)
    ret.BlockSize = int64(info.mBlockSize)
    ret.Owner = C.GoString(info.mOwner)
    ret.Group = C.GoString(info.mGroup)
    ret.Permissions = int16(info.mPermissions)
    ret.LastAccess = time.Unix(int64(info.mLastAccess), int64(0))
    return ret, nil
}

/** 
 * GetHosts - Get hostnames where a particular block (determined by
 * pos & blocksize) of a file is stored.
 * @param fs The configured filesystem handle.
 * @param path The path of the file. 
 * @param start The start of the block.
 * @param length The length of the block.
 * @return Returns a 2-D slice of blocks-hosts, or nil on error.
 */
func (fs *Fs) GetHosts(path string, start, length int64) ([][]string, error) {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    ret, err := C.hdfsGetHosts(fs.cptr, p, C.tOffset(start), C.tOffset(length))
    if ret == (***C.char)(unsafe.Pointer(uintptr(0))) {
        return nil, err
    }
    defer C.hdfsFreeHosts(ret)
    i := int(C.getlen(ret))
    j := int(C.getl(ret))
    s := make([][]string, i)
    for k, _ := range s {
        s[k] = make([]string, j)
        for p, _ := range s[k] {
            s[k][p] = C.GoString(C.getstring(ret, C.int(k), C.int(p)))
        }
    }
    return s, nil
}

/** 
 * GetDefaultBlockSize - Get the optimum blocksize.
 * @param fs The configured filesystem handle.
 * @return Returns the blocksize; -1 on error. 
 */
func (fs *Fs) GetDefaultBlockSize() (int64, error) {
    ret, err := C.hdfsGetDefaultBlockSize(fs.cptr)
    return int64(ret), err
}

/** 
 * GetCapacity - Return the raw capacity of the filesystem.  
 * @param fs The configured filesystem handle.
 * @return Returns the raw-capacity; -1 on error. 
 */
// work around: fix ESRCH
func (fs *Fs) GetCapacity() (int64, error) {
    ret, err := C.hdfsGetCapacity(fs.cptr)
    if ret == C.tOffset(-1) {
        return -1, err
    }
    return int64(ret), nil
}

/** 
 * GetUsed - Return the total raw size of all files in the filesystem.
 * @param fs The configured filesystem handle.
 * @return Returns the total-size; check on error. 
 */
func (fs *Fs) GetUsed() (int64, error) {
    ret, err := C.hdfsGetUsed(fs.cptr)
    return int64(ret), err
}

/** 
 * Chown 
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param owner this is a string in Hadoop land. Set to "" if only setting group
 * @param group  this is a string in Hadoop land. Set to "" if only setting user
 * @return nil on success else error
 */
func (fs *Fs) Chown(path, owner, group string) error {
    p, o, g := C.CString(path), C.CString(owner), C.CString(group)
    defer C.free(unsafe.Pointer(p))
    defer C.free(unsafe.Pointer(o))
    defer C.free(unsafe.Pointer(g))
    _, err := C.hdfsChown(fs.cptr, p, o, g)
    return err
}

/** 
 * Chmod
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mode the bitmask to set it to
 * @return nil on success else error
 */
func (fs *Fs) Chmod(path string, mode int16) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsChmod(fs.cptr, p, C.short(mode))
    return err
}

/** 
 * Utime
 * @param fs The configured filesystem handle.
 * @param path the path to the file or directory
 * @param mtime new modification time or 0 for only set access time in seconds
 * @param atime new access time or 0 for only set modification time in seconds
 * @return nil on success else error
 */
func (fs *Fs) Utime(path string, mtime, atime time.Time) error {
    p := C.CString(path)
    defer C.free(unsafe.Pointer(p))
    _, err := C.hdfsUtime(fs.cptr, p, C.tTime(mtime.Unix()), C.tTime(atime.Unix()))
    return err
}
