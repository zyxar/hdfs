package hdfs

// #cgo linux CFLAGS: -I/opt/jdk/include -I/opt/jdk/include/linux
// #cgo linux LDFLAGS: -Llib -lhdfs -L/opt/jdk/jre/lib/amd64/server -ljvm
// #cgo darwin LDFLAGS: -L/usr/lib/java -lhdfs -framework JavaVM
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
	"fmt"
	"sync"
	"time"
	"unsafe"
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
	*sync.RWMutex
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

func (info *FileInfo) String() (ret string) {
	ret = fmt.Sprintf("%-8s\t:  %s\n", "Name", info.Name) +
		fmt.Sprintf("%-8s\t:  %c\n", "Type", info.Kind) +
		fmt.Sprintf("%-8s\t:  %d\n", "Replication", info.Replication) +
		fmt.Sprintf("%-8s\t:  %v\n", "BlockSize", info.BlockSize) +
		fmt.Sprintf("%-8s\t:  %v\n", "Size", info.Size) +
		fmt.Sprintf("%-8s\t:  %v\n", "LastMod", info.LastMod) +
		fmt.Sprintf("%-8s\t:  %v\n", "LastAccess", info.LastAccess) +
		fmt.Sprintf("%-8s\t:  %s\n", "Owner", info.Owner) +
		fmt.Sprintf("%-8s\t:  %s\n", "Group", info.Group) +
		fmt.Sprintf("%-8s\t:  %b\n", "Permissions", info.Permissions)
	return
}

//Factory method for get a *hdfs.Fs handle: connect to a hdfs file system as a specific user.
//host: A string containing either a host name, or an ip address of the namenode of a hdfs cluster. 'host' should be passed as "" if you want to connect to local filesystem. 'host' should be passed as 'default' (and port as 0) to used the 'configured' filesystem (core-site/core-default.xml).
//port: The port on which the server is listening.
//user: the user name (this is hadoop domain user). Or "" is equivelant to Connect(host, port).
//Returns a handle to the filesystem or nil on error.
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
	if err != nil && ret == (_Ctype_hdfsFS)(unsafe.Pointer(uintptr(0))) {
		return nil, err
	}
	return &Fs{ret}, nil
}

//Factory method for get a *hdfs.Fs handle: connect to a hdfs file system.
//host: A string containing either a host name, or an ip address of the namenode of a hdfs cluster. 'host' should be passed as "" if you want to connect to local filesystem. 'host' should be passed as 'default' (and port as 0) to used the 'configured' filesystem (core-site/core-default.xml).
//port: The port on which the server is listening.
//Returns a handle to the filesystem or nil on error.
func Connect(host string, port uint16) (*Fs, error) {
	return ConnectAsUser(host, port, "")
}

//Disconnect from the hdfs file system.
//Returns nil on success, else error
func (fs *Fs) Disconnect() error {
	ret, err := C.hdfsDisconnect(fs.cptr)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

func Disconnect(fs *Fs) error {
	return fs.Disconnect()
}

//Open a hdfs file in given mode.
//path: The full path to the file.
//flags: - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT), O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return nil and set error equal ENOTSUP.
//bufferSize: Size of buffer for read/write - pass 0 if you want to use the default configured values.
//replication: Block replication - pass 0 if you want to use the default configured values.
//blocksize: Size of block - pass 0 if you want to use the default configured values.
//Returns the handle to the open file or nil on error.
func (fs *Fs) OpenFile(path string, flags int, buffersize int, replication int, blocksize uint32) (*File, error) {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	file, err := C.hdfsOpenFile(fs.cptr, p, C.int(flags), C.int(buffersize), C.short(replication), C.tSize(blocksize))
	if err != nil && file == (C.hdfsFile)(unsafe.Pointer(uintptr(0))) {
		return nil, err
	}
	return &File{file, new(sync.RWMutex)}, nil
}

//Close an open file. 
//file: The file handle.
//Returns nil on success, or error.  
func (fs *Fs) CloseFile(file *File) error {
	ret, err := C.hdfsCloseFile(fs.cptr, file.cptr)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Checks if a given path exsits on the filesystem.
//path: The path to look for.
//Returns nil on success, or error.
func (fs *Fs) Exists(path string) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsExists(fs.cptr, p)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Seek to given offset in file. This works only for files opened in read-only mode. 
//file: The file handle.
//pos: Offset into the file to seek into.
//Returns nil on success, or error.  
func (fs *Fs) Seek(file *File, pos int64) error {
	file.Lock()
	defer file.Unlock()
	ret, err := C.hdfsSeek(fs.cptr, file.cptr, C.tOffset(pos))
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Get the current offset in the file, in bytes.
//file: The file handle.
//Returns current offset, or error.
func (fs *Fs) Tell(file *File) (int64, error) {
	ret, err := C.hdfsTell(fs.cptr, file.cptr)
	if err != nil && ret == C.tOffset(-1) {
		return -1, err
	}
	return int64(ret), nil
}

//Read data from an open file.
//file: The file handle.
//buffer: The buffer to copy read bytes into.
//length: The length of the buffer.
//Returns the number of bytes actually read, possibly less than than length; or error.
func (fs *Fs) Read(file *File, buffer []byte, length int) (uint32, error) {
	file.RLock()
	defer file.RUnlock()
	ret, err := C.hdfsRead(fs.cptr, file.cptr, (unsafe.Pointer(&buffer[0])), C.tSize(length))
	if err != nil && ret == C.tSize(-1) {
		return 0, err
	}
	return uint32(ret), nil
}

//Positional read of data from an open file.
//file: The file handle.
//position: Position from which to read.
//buffer: The buffer to copy read bytes into.
//length: The length of the buffer.
//Returns the number of bytes actually read, possibly less than length; or error.
func (fs *Fs) Pread(file *File, position int64, buffer []byte, length int) (uint32, error) {
	file.RLock()
	defer file.RUnlock()
	ret, err := C.hdfsPread(fs.cptr, file.cptr, C.tOffset(position), (unsafe.Pointer(&buffer[0])), C.tSize(length))
	if err != nil && ret == C.tSize(-1) {
		return 0, err
	}
	return uint32(ret), nil
}

//Write data into an open file.
//file: The file handle.
//buffer: The data.
//length: The no. of bytes to write. 
//Returns the number of bytes written; or error.
func (fs *Fs) Write(file *File, buffer []byte, length int) (uint32, error) {
	file.Lock()
	defer file.Unlock()
	ret, err := C.hdfsWrite(fs.cptr, file.cptr, (unsafe.Pointer(&buffer[0])), C.tSize(length))
	if err != nil && ret == C.tSize(-1) {
		return 0, err
	}
	return uint32(ret), nil
}

//Flush the data. 
//file: The file handle.
//Returns nil on success, or error. 
func (fs *Fs) Flush(file *File) error {
	file.Lock()
	defer file.Unlock()
	ret, err := C.hdfsFlush(fs.cptr, file.cptr)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Number of bytes that can be read from this input stream without blocking.
//file: The file handle.
//Returns available bytes; or error. 
func (fs *Fs) Available(file *File) (uint32, error) {
	file.RLock()
	defer file.RUnlock()
	ret, err := C.hdfsAvailable(fs.cptr, file.cptr)
	if err != nil && ret == C.int(-1) {
		return 0, err
	}
	return uint32(ret), nil
}

//Copy file from one filesystem to another.
//src: The path of source file. 
//dstFS: The handle to destination filesystem.
//dst: The path of destination file. 
//Returns nil on success, or error. 
func (fs *Fs) Copy(src string, dstFS *Fs, dst string) error {
	srcstr := C.CString(src)
	dststr := C.CString(dst)
	defer C.free(unsafe.Pointer(srcstr))
	defer C.free(unsafe.Pointer(dststr))
	ret, err := C.hdfsCopy(fs.cptr, srcstr, dstFS.cptr, dststr)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Move file from one filesystem to another.
//src: The path of source file. 
//dstFS: The handle to destination filesystem.
//dst: The path of destination file. 
//Returns nil on success, or error. 
func (fs *Fs) Move(src string, dstFS *Fs, dst string) error {
	srcstr := C.CString(src)
	dststr := C.CString(dst)
	defer C.free(unsafe.Pointer(srcstr))
	defer C.free(unsafe.Pointer(dststr))
	ret, err := C.hdfsMove(fs.cptr, srcstr, dstFS.cptr, dststr)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Delete file. 
//path: The path of the file. 
//Returns nil on success, or error. 
func (fs *Fs) Delete(path string) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsDelete(fs.cptr, p)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Rename file. 
//oldpath: The path of the source file. 
//newpath: The path of the destination file. 
//Returns nil on success, or error. 
func (fs *Fs) Rename(oldpath, newpath string) error {
	op, np := C.CString(oldpath), C.CString(newpath)
	defer C.free(unsafe.Pointer(op))
	defer C.free(unsafe.Pointer(np))
	ret, err := C.hdfsRename(fs.cptr, op, np)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Get the current working directory for the given filesystem.
//buffer: The user-buffer to copy path of cwd into. 
//size: The length of user-buffer.
//Returns buffer, or error.
func (fs *Fs) GetWorkingDirectory(buffer []byte, size uint32) ([]byte, error) {
	_, err := C.hdfsGetWorkingDirectory(fs.cptr, (*C.char)(unsafe.Pointer(&buffer[0])), C.size_t(size))
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

//Set the working directory. All relative paths will be resolved relative to it.
//path: The path of the new 'cwd'. 
//Returns nil on success, or error. 
func (fs *Fs) SetWorkingDirectory(path string) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsSetWorkingDirectory(fs.cptr, p)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Make the given file and all non-existent parents into directories.
//path: The path of the directory. 
//Returns nil on success, or error. 
func (fs *Fs) CreateDirectory(path string) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsCreateDirectory(fs.cptr, p)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Set the replication of the specified file to the supplied value.
//path: The path of the file. 
//Returns nil on success, or error. 
func (fs *Fs) SetReplication(path string, replication int16) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsSetReplication(fs.cptr, p, C.int16_t(replication))
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Get list of files/directories for a given directory-path.
//path: The path of the directory. 
//Returns a slice of FileInfo struct pointer, or nil on error.
func (fs *Fs) ListDirectory(path string) ([]*FileInfo, error) {
	var num int
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	info, _ := C.hdfsListDirectory(fs.cptr, p, (*C.int)(unsafe.Pointer(&num)))
	if info == nil {
		return nil, fmt.Errorf("error in listing directory %s", path)
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

//Get information about a path as a single FileInfo struct pointer.
//path: The path of the file. 
//Returns a pointer to FileInfo object, or nil on error.
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

//Get hostnames where a particular block (determined by pos & blocksize) of a file is stored.
//path: The path of the file. 
//start: The start of the block.
//length: The length of the block.
//Returns a 2-D slice of blocks-hosts, or nil on error.
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

//Get the optimum blocksize.
//Returns the blocksize; -1 on error. 
func (fs *Fs) GetDefaultBlockSize() (int64, error) {
	ret, err := C.hdfsGetDefaultBlockSize(fs.cptr)
	if err != nil && ret == C.tOffset(-1) {
		return -1, err
	}
	return int64(ret), nil
}

//Get the raw capacity of the filesystem.  
//Returns the raw-capacity; -1 on error. 
func (fs *Fs) GetCapacity() (int64, error) {
	ret, err := C.hdfsGetCapacity(fs.cptr)
	if err != nil && ret == C.tOffset(-1) {
		return -1, err
	}
	return int64(ret), nil
}

//Get the total raw size of all files in the filesystem.
//Returns the total-size; check on error. 
func (fs *Fs) GetUsed() (int64, error) {
	ret, err := C.hdfsGetUsed(fs.cptr)
	if err != nil && ret == C.tOffset(-1) {
		return -1, err
	}
	return int64(ret), nil
}

//Chown.
//path: the path to the file or directory.
//owner: this is a string in Hadoop land. Set to "" if only setting group.
//group:  this is a string in Hadoop land. Set to "" if only setting user.
//Returns nil on success else error.
func (fs *Fs) Chown(path, owner, group string) error {
	p, o, g := C.CString(path), C.CString(owner), C.CString(group)
	defer C.free(unsafe.Pointer(p))
	defer C.free(unsafe.Pointer(o))
	defer C.free(unsafe.Pointer(g))
	ret, err := C.hdfsChown(fs.cptr, p, o, g)
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Chmod.
//path: the path to the file or directory.
//mode: the bitmask to set it to.
//Returns nil on success else error.
func (fs *Fs) Chmod(path string, mode int16) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsChmod(fs.cptr, p, C.short(mode))
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}

//Utime.
//path: the path to the file or directory.
//mtime: new modification time or 0 for only set access time in seconds.
//atime: new access time or 0 for only set modification time in seconds.
//Returns nil on success else error.
func (fs *Fs) Utime(path string, mtime, atime time.Time) error {
	p := C.CString(path)
	defer C.free(unsafe.Pointer(p))
	ret, err := C.hdfsUtime(fs.cptr, p, C.tTime(mtime.Unix()), C.tTime(atime.Unix()))
	if err != nil && ret == C.int(-1) {
		return err
	}
	return nil
}
