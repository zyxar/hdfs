package hdfs

import (
	"fmt"
	"testing"
	"time"
)

const (
	server = "brick5"
	ssport = 34310
)

func TestConn(t *testing.T) {
	fs, err := Connect(server, ssport)
	if err != nil {
		t.Errorf("Error on connecting to hdfs: %v\n", err)
		return
	}
	defer fs.Disconnect()

	lfs, err := Connect("", 0)
	if err != nil {
		t.Errorf("Error on connecting to local hdfs: %v\n", err)
		return
	}

	err = lfs.Disconnect()
	if err != nil {
		t.Errorf("Error on disconnecting local hdfs: %v\n", err)
	}
}

func TestWrite(t *testing.T) {
	writePath := "/tmp/gotestfile.txt"
	buf := []byte("hello hdfs world, from go!")

	err := func() error {
		fs, err := Connect(server, ssport)
		if err != nil {
			return fmt.Errorf("Error on connecting to hdfs: %v\n", err)
		}
		defer fs.Disconnect()

		file, err := fs.OpenFile(writePath, O_WRONLY|O_CREATE, 0, 0, 0)
		if err != nil {
			return fmt.Errorf("Error on opening file: %v\n", err)
		}

		size, err := fs.Write(file, buf, len(buf))
		if err != nil {
			return fmt.Errorf("Error on writing bytes to file: %v\n", err)
		} else {
			fmt.Printf("\tWrote %d bytes\n", size)
		}
		pos, err := fs.Tell(file)
		if err != nil {
			return fmt.Errorf("Error on getting current file position: %v. Got %v\n", err, pos)
		}
		err = fs.Flush(file)
		if err != nil {
			return fmt.Errorf("Error on flushing file: %v\n", err)
		}
		err = fs.CloseFile(file)
		if err != nil {
			return fmt.Errorf("Error on closing file: %v\n", err)
		}
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestRead(t *testing.T) {
	pos := int64(18)
	buffer := make([]byte, 32)
	readPath := "/tmp/gotestfile.txt"

	err := func() error {
		fs, err := Connect(server, ssport)
		if err != nil {
			return fmt.Errorf("Error on connecting to hdfs: %v\n", err)
		}
		defer fs.Disconnect()

		file, err := fs.OpenFile(readPath, O_RDONLY, 0, 0, 0)
		if err != nil {
			return fmt.Errorf("Error on opening file: %v %v\n", file, err)
		}
		val, err := fs.Available(file)
		if err != nil {
			return fmt.Errorf("Error on getting file availability: %v\n", err)
		} else {
			fmt.Printf("\tFile availability: %v\n", val)
		}
		err = fs.Seek(file, pos)
		if err != nil {
			return fmt.Errorf("Error on seeking for reading: %v\n", err)
		}
		cpos, err := fs.Tell(file)
		if err != nil {
			return fmt.Errorf("Error on getting current file position: %v\n", err)
		}
		if cpos != pos {
			return fmt.Errorf("Error on getting current file position: Set = %v, Got = %v\n", pos, cpos)
		}
		val, err = fs.Read(file, buffer, len(buffer))
		if err != nil {
			return fmt.Errorf("Error on reading file: %v\n", err)
		} else {
			fmt.Printf("\tREAD: %s\n", buffer)
		}
		val, err = fs.Pread(file, 0, buffer, len(buffer))
		if err != nil {
			return fmt.Errorf("Error on preading file: %v\n", err)
		} else {
			fmt.Printf("\tPREAD: %s\n", buffer)
		}
		err = fs.CloseFile(file)
		if err != nil {
			return fmt.Errorf("Error on closing file: %v\n", err)
		}
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestCopyMove(t *testing.T) {
	fs, err := Connect(server, ssport)
	if err != nil {
		t.Errorf("Error on connecting to hdfs: %v\n", err)
		return
	}
	defer fs.Disconnect()

	srcPath := "/tmp/gotestfile.txt"
	dstPath := "/tmp/gotestfile2.txt"

	lfs, err := Connect("", 0)
	if err != nil {
		t.Errorf("Error on connecting to local hdfs: %v\n", err)
		return
	}

	defer lfs.Disconnect()

	// remote
	err = func() error {
		err := fs.Copy(srcPath, fs, dstPath)
		if err != nil {
			return fmt.Errorf("Error on copying remote to remote: %v\n", err)
		}
		err = fs.Delete(dstPath)
		if err != nil {
			return fmt.Errorf("Error on delete file: %v\n", err)
		}
		err = fs.Move(srcPath, fs, dstPath)
		if err != nil {
			return fmt.Errorf("Error on moving remote to remote: %v\n", err)
		}
		err = fs.Rename(dstPath, srcPath)
		if err != nil {
			return fmt.Errorf("Error on renaming file: %v\n", err)
		}
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}

	// remote - local
	err = func() error {
		err := fs.Copy(srcPath, lfs, dstPath)
		if err != nil {
			return fmt.Errorf("Error on copying remote to local: %v\n", err)
		}
		err = lfs.Delete(dstPath)
		if err != nil {
			return fmt.Errorf("Error on delete local file: %v\n", err)
		}
		err = fs.Copy(srcPath, fs, dstPath)
		if err != nil {
			return fmt.Errorf("Error on copying remote to remote: %v\n", err)
		}
		err = fs.Move(dstPath, lfs, dstPath)
		if err != nil {
			return fmt.Errorf("Error on moving remote to local: %v\n", err)
		}
		err = lfs.Move(dstPath, fs, dstPath)
		if err != nil {
			return fmt.Errorf("Error on moving local to remote: %v\n", err)
		}
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}
	fs.Delete(dstPath)
}

func TestDir(t *testing.T) {
	slashTmp := "/tmp"
	directory := "/tmp/newdir"
	srcPath := "/tmp/gotestfile.txt"
	buffer := make([]byte, 256)

	fs, err := Connect(server, ssport)
	if err != nil {
		t.Errorf("Error on connecting to hdfs: %v\n", err)
		return
	}
	defer fs.Disconnect()

	err = func() error {
		err := fs.CreateDirectory(directory)
		if err != nil {
			return fmt.Errorf("Error on creating directory: %v\n", err)
		}
		err = fs.SetReplication(srcPath, 2)
		if err != nil {
			return fmt.Errorf("Error on setting replication: %v\n", err)
		}
		_, err = fs.GetWorkingDirectory(buffer, uint32(len(buffer)))
		if err != nil {
			return fmt.Errorf("Error on getting working directory: %v\n", err)
		} else {
			fmt.Printf("\tGot working directory: %s\n", buffer)
		}
		err = fs.SetWorkingDirectory(slashTmp)
		if err != nil {
			return fmt.Errorf("Error on setting working directory: %v\n", err)
		}
		_, err = fs.GetWorkingDirectory(buffer, uint32(len(buffer)))
		if err != nil {
			return fmt.Errorf("Error on getting working directory: %v\n", err)
		} else {
			fmt.Printf("\tGot working directory: %s\n", buffer)
		}
		val, err := fs.GetDefaultBlockSize()
		if err != nil {
			return fmt.Errorf("Error on getting default block size: %v\n", err)
		} else {
			fmt.Printf("\tGot default block size: %v\n", val)
		}
		val, err = fs.GetCapacity()
		if err != nil {
			return fmt.Errorf("Error on getting capacity: %v\n", err)
		} else {
			fmt.Printf("\tGot capacity: %v\n", val)
		}
		val, err = fs.GetUsed()
		if err != nil {
			return fmt.Errorf("Error on getting used: %v\n", err)
		} else {
			fmt.Printf("\tGot used: %v\n\n", val)
		}

		info, err := fs.GetPathInfo(slashTmp)
		if err != nil {
			return fmt.Errorf("Error on getting path info: %v %v\n", info, err)
		} else {
			fmt.Printf("%s\n", info)
		}

		ifo, err := fs.ListDirectory(slashTmp)
		if err != nil {
			return fmt.Errorf("Error on listing directory: %v\n", err)
		} else {
			for _, v := range ifo {
				fmt.Printf("%s\n", v)
				//fmt.Printf("\tmeta: %p\n", v.meta.cptr)
			}
		}

		var hosts [][]string
		hosts, err = fs.GetHosts(srcPath, 0, 1)
		if err != nil {
			return fmt.Errorf("Error on getting hosts: %v\n", err)
		} else {
			for _, v := range hosts {
				for _, k := range v {
					fmt.Printf("\thost - %s\n", k)
				}
			}
			fmt.Printf("\n")
		}
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}

	newOwner := "root"
	newPerm := int16(0666)
	err = func() error {
		err := fs.Chown(srcPath, "", "users")
		if err != nil {
			return fmt.Errorf("Error on changing owner: %v\n", err)
		}
		err = fs.Chown(srcPath, newOwner, "")
		if err != nil {
			return fmt.Errorf("Error on changing owner: %v\n", err)
		}
		err = fs.Chmod(srcPath, newPerm)
		if err != nil {
			return fmt.Errorf("Error on changing mode: %v\n", err)
		}
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}

	newMtime := time.Now()
	newAtime := time.Now()

	err = func() error {
		err := fs.Utime(srcPath, newMtime, newAtime)
		if err != nil {
			t.Errorf("Error on setting utime: %v\n", err)
		}

		// check chown, chmod, utime
		info, err := fs.GetPathInfo(srcPath)
		if err != nil {
			return fmt.Errorf("Error on getting path info: %v %v\n", info, err)
		}
		if info.Owner != newOwner {
			return fmt.Errorf("Chown - owner failed\n")
		}
		if info.Permissions != newPerm {
			return fmt.Errorf("Chmod - permission failed\n")
		}
		if info.LastMod.Unix() != newMtime.Unix() {
			return fmt.Errorf("Utime - mtime failed: %v - %v\n", info.LastMod, newMtime)
		}
		if info.LastAccess.Unix() != newAtime.Unix() {
			return fmt.Errorf("Utime - atime failed: %v - %v\n", info.LastAccess, newAtime)
		}

		// cleanup
		err = fs.Delete(directory)
		if err != nil {
			return fmt.Errorf("Error on delete directory: %v\n", err)
		}
		err = fs.Delete(srcPath)
		if err != nil {
			return fmt.Errorf("Error on delete file: %v\n", err)
		}
		err = fs.Exists(directory)
		if err != nil {
			return fmt.Errorf("Error on testing directory existence: %v\n", err)
		}
		err = fs.Exists(srcPath)
		if err != nil {
			return fmt.Errorf("Error on testing file existence: %v\n", err)
		}
		return nil
	}()
}

func TestAppend(t *testing.T) {
	writePath := "/tmp/appends"
	buf := []byte("hello,")

	err := func() error {
		fs, err := Connect(server, ssport)
		if err != nil {
			return fmt.Errorf("Error on connecting to hdfs: %v\n", err)
		}
		defer fs.Disconnect()
		file, err := fs.OpenFile(writePath, O_WRONLY, 0, 0, 0)
		if err != nil {
			return fmt.Errorf("Error on opening file for writing: %v\n", err)
		}
		_, err = fs.Write(file, buf, len(buf))
		if err != nil {
			return fmt.Errorf("Error on writing bytes to file: %v\n", err)
		}
		err = fs.Flush(file)
		if err != nil {
			return fmt.Errorf("Error on flushing file: %v\n", err)
		}
		err = fs.CloseFile(file)
		if err != nil {
			return fmt.Errorf("Error on closing file: %v\n", err)
		}

		file, err = fs.OpenFile(writePath, O_WRONLY|O_APPEND, 0, 0, 0)
		if err != nil {
			return fmt.Errorf("Error on opening file for writing: %v\n", err)
		}
		buf = []byte(" from go users!")
		_, err = fs.Write(file, buf, len(buf))
		if err != nil {
			return fmt.Errorf("Error on writing bytes to file: %v\n", err)
		}
		err = fs.Flush(file)
		if err != nil {
			return fmt.Errorf("Error on flushing file: %v\n", err)
		}
		err = fs.CloseFile(file)
		if err != nil {
			return fmt.Errorf("Error on closing file: %v\n", err)
		}
		// check size
		info, err := fs.GetPathInfo(writePath)
		if err != nil {
			return fmt.Errorf("Error on getting path info: %v %v\n", info, err)
		} else {
			fmt.Printf("%s\n", info)
		}
		if info.Size != int64(len("hello, from go users!")) {
			return fmt.Errorf("Appended file size not correct\n")
		}
		rdbuf := make([]byte, 32)

		file, err = fs.OpenFile(writePath, O_RDONLY, 0, 0, 0)
		if err != nil {
			return fmt.Errorf("Error on opening file for reading: %v %v\n", file, err)
		}
		val, err := fs.Read(file, rdbuf, len(rdbuf))
		if err != nil {
			return fmt.Errorf("Error on reading file: %v\n", err)
		}
		if string(rdbuf[:val]) != "hello, from go users!" {
			return fmt.Errorf("Appended file content not correct\n")
		}
		err = fs.CloseFile(file)
		if err != nil {
			return fmt.Errorf("Error on closing file: %v\n", err)
		}
		fs.Delete(writePath)
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestOnUser1(t *testing.T) {
	fs, err := Connect(server, ssport)
	if err != nil {
		t.Errorf("Error on connecting to hdfs: %v\n", err)
		return
	}
	defer fs.Disconnect()

	err = fs.Chmod("/tmp/", int16(0777))
	if err != nil {
		t.Errorf("Error on changing mode to 777: %v\n", err)
	}
}

func TestOnUser2(t *testing.T) {
	tuser := "nobody"
	writePath := "/tmp/gousertextfile.txt"
	buf := []byte("hello hdfs world, from go users!")
	err := func() error {
		fs, err := ConnectAsUser(server, ssport, tuser)
		if err != nil {
			return fmt.Errorf("Error on connecting as user %s: %v\n", tuser, err)
		}
		defer fs.Disconnect()
		file, err := fs.OpenFile(writePath, O_WRONLY|O_CREATE, 0, 0, 0)
		if err != nil {
			return fmt.Errorf("Error on opening file for writing: %v\n", err)
		}

		_, err = fs.Write(file, buf, len(buf))
		if err != nil {
			return fmt.Errorf("Error on writing bytes to file: %v\n", err)
		}
		pos, err := fs.Tell(file)
		if err != nil {
			return fmt.Errorf("Error on getting current file position: %v. Got %v\n", err, pos)
		}
		err = fs.Flush(file)
		if err != nil {
			return fmt.Errorf("Error on flushing file: %v\n", err)
		}
		err = fs.CloseFile(file)
		if err != nil {
			return fmt.Errorf("Error on closing file: %v\n", err)
		}

		info, err := fs.GetPathInfo(writePath)
		if err != nil {
			return fmt.Errorf("Error on getting path info: %v %v\n", info, err)
		} else {
			fmt.Printf("%s\n", info)
		}
		if info.Owner != tuser {
			return fmt.Errorf("HDFS new file user is not correct\n")
		}
		fs.Delete(writePath)
		return nil
	}()
	if err != nil {
		t.Errorf("%v", err)
	}
}

func TestCleanup(t *testing.T) {
	fs, err := Connect(server, ssport)
	if err != nil {
		t.Errorf("Error on connecting to hdfs: %v\n", err)
		return
	}
	defer fs.Disconnect()

	fs.Delete("/tmp")
}
