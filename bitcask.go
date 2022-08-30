package bitcask

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const maxFileSize = 1024

const (
	ReadOnly     ConfigOpt = 0
	ReadWrite    ConfigOpt = 1
	SyncOnPut    ConfigOpt = 2
	SyncOnDemand ConfigOpt = 3

	KeyDoesNotExist     = "key does not exist"
	CannotOpenThisDir   = "cannot open this directory"
	WriteDenied         = "write permission denied"
	CannotCreateBitcask = "read only cannot create new bitcask directory"
	WriterExist         = "another writer exists in this bitcask"
)

const (
	dirMode  = os.FileMode(0777)
	fileMode = os.FileMode(0666)

	keyDirFilePrefix = "keydir"
	hintFilePrefix   = "hintfile"

	staticFields    = 3
	numberFieldSize = 19

	reader    processAccess = 0
	writer    processAccess = 1
	noProcess processAccess = 2

	readLock  = ".readlock"
	writeLock = ".writelock"
	tompStone = "DELETE THIS VALUE"
)

type ConfigOpt int

type BitcaskError string

type processAccess int

type pendingWrites map[string]string

type Bitcask struct {
	directoryPath string
	lock          string
	keyDirFile    string
	keyDir        map[string]record
	config        options
	activeFile    dataFile
}

type dataFile struct {
	file        *os.File
	fileName    string
	currentPos  int64
	currentSize int64
}

type record struct {
	fileId    string
	valueSize int64
	valuePos  int64
	tstamp    int64
}

type options struct {
	writePermission ConfigOpt
	syncOption      ConfigOpt
}

func (e BitcaskError) Error() string {
	return string(e)
}

// Open creates a new process to manipulate the given bitcask datastore path.
// It takes options ReadWrite, ReadOnly, SyncOnPut and SyncOnDemand.
// Only one ReadWrite process can open a bitcask at a time.
// Only ReadWrite permission can create a new bitcask datastore.
// If there is no bitcask datastore in the given path a new datastore is created when ReadWrite permission is given.
func Open(dirPath string, opts ...ConfigOpt) (*Bitcask, error) {
	bitcask := Bitcask{
		keyDir:        make(map[string]record),
		directoryPath: dirPath,
		config:        options{writePermission: ReadOnly, syncOption: SyncOnDemand},
	}

	for _, opt := range opts {
		switch opt {
		case ReadWrite:
			bitcask.config.writePermission = ReadWrite
		case SyncOnPut:
			bitcask.config.syncOption = SyncOnPut
		}
	}

	_, openErr := os.Open(dirPath)

	if openErr == nil {
		if bitcask.lockCheck() == writer {
			return nil, BitcaskError(WriterExist)
		}
		bitcask.buildKeyDir()
		if bitcask.config.writePermission == ReadOnly {
			bitcask.buildKeyDirFile()
			bitcask.lock = readLock + strconv.Itoa(int(time.Now().UnixMicro()))
			lockFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, bitcask.lock),
				os.O_CREATE, fileMode)
			lockFile.Close()
		} else {
			bitcask.lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
			lockFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, bitcask.lock),
				os.O_CREATE, fileMode)
			lockFile.Close()
			bitcask.createActiveFile()
		}

	} else if os.IsNotExist(openErr) {
		if bitcask.config.writePermission == ReadOnly {
			return nil, BitcaskError(CannotCreateBitcask)
		}
		os.MkdirAll(dirPath, dirMode)
		bitcask.keyDir = make(map[string]record)
		bitcask.createActiveFile()
		bitcask.lock = writeLock + strconv.Itoa(int(time.Now().UnixMicro()))
		lockFile, _ := os.OpenFile(path.Join(bitcask.directoryPath, bitcask.lock),
			os.O_CREATE, fileMode)
		lockFile.Close()
	} else {
		return nil, BitcaskError(fmt.Sprintf("%s: %s", dirPath, CannotOpenThisDir))
	}
	return &bitcask, nil
}

// Get retrieves the value by key from a bitcask datastore.
// returns an error if key does not exist in the bitcask datastore.
func (b *Bitcask) Get(key string) (string, error) {
	recValue, isExist := b.keyDir[key]

	if !isExist {
		return "", BitcaskError(fmt.Sprintf("%s: %s", string(key), KeyDoesNotExist))
	}
	buf := make([]byte, recValue.valueSize)
	file, _ := os.Open(path.Join(b.directoryPath, recValue.fileId))
	defer file.Close()
	file.ReadAt(buf, int64(recValue.valuePos))
	return string(buf), nil
}

// Put stores a value by key in a bitcask datastore.
// Sync on each put if SyncOnPut option is set.
func (b *Bitcask) Put(key string, value string) error {
	if b.config.writePermission == ReadOnly {
		return BitcaskError(WriteDenied)
	}

	tstamp := time.Now().UnixMicro()

	n, err := b.writeToActiveFile(string(compressFileLine(key, value, tstamp)))
	if err != nil {
		return err
	}

	b.keyDir[key] = record{
		fileId:    b.activeFile.fileName,
		valueSize: int64(len(value)),
		valuePos:  b.activeFile.currentPos + staticFields*numberFieldSize + int64(len(key)),
		tstamp:    tstamp,
	}

	b.activeFile.currentPos += n
	b.activeFile.currentSize += n

	if b.config.syncOption == SyncOnPut {
		b.Sync()
	}
	return nil
}

// Delete removes a key from a bitcask datastore
// by appending a special TompStone value that will be deleted in the next merge.
// returns an error if key does not exist in the bitcask datastore.
func (b *Bitcask) Delete(key string) error {
	if b.config.writePermission == ReadOnly {
		return BitcaskError(WriteDenied)
	}

	_, err := b.Get(key)
	if err != nil {
		return err
	}

	delete(b.keyDir, key)
	return nil
}

// ListKeys list all keys in a bitcask datastore.
func (b *Bitcask) ListKeys() []string {
	var list []string

	for key := range b.keyDir {
		list = append(list, key)
	}
	return list
}

// Fold folds over all key/value pairs in a bitcask datastore.
// fun is expected to be in the form: F(K, V, Acc) -> Acc
func (b *Bitcask) Fold(fun func(string, string, any) any, acc any) any {
	for key := range b.keyDir {
		value, _ := b.Get(key)
		acc = fun(key, value, acc)
	}
	return acc
}

// Merge rearrange the bitcask datastore in a more compact form.
// Also produces hintfiles to provide a faster startup.
// returns an error if ReadWrite permission is not set.
func (b *Bitcask) Merge() error {
	if b.config.writePermission == ReadOnly {
		return BitcaskError(WriteDenied)
	}

	var currentPos int64 = 0
	var currentSize int64 = 0
	var oldFiles []string
	newKeyDir := make(map[string]record)

	mergeFileName := strconv.FormatInt(time.Now().UnixMicro(), 10)
	hintFileName := hintFilePrefix + mergeFileName
	b.Sync()

	mergeFile, _ := os.OpenFile(path.Join(b.directoryPath, mergeFileName),
		os.O_CREATE|os.O_RDWR, fileMode)

	hintFile, _ := os.OpenFile(path.Join(b.directoryPath, hintFileName),
		os.O_CREATE|os.O_RDWR, fileMode)

	bitcaskDir, _ := os.Open(b.directoryPath)
	files, _ := bitcaskDir.Readdir(0)
	for _, file := range files {
		if file.Name() != b.activeFile.fileName {
			oldFiles = append(oldFiles, file.Name())
		}
	}

	for key, recValue := range b.keyDir {
		if recValue.fileId != b.activeFile.fileName {

			tstamp := time.Now().UnixMicro()
			value, _ := b.Get(key)
			fileLine := string(compressFileLine(key, value, tstamp))

			if int64(len(fileLine))+currentSize > maxFileSize {
				mergeFile.Close()
				hintFile.Close()

				mergeFileName = strconv.FormatInt(time.Now().UnixMicro(), 10)
				mergeFile, _ = os.OpenFile(path.Join(b.directoryPath, mergeFileName),
					os.O_CREATE|os.O_RDWR, fileMode)

				hintFileName = hintFilePrefix + mergeFileName
				hintFile, _ = os.OpenFile(path.Join(b.directoryPath, hintFileName),
					os.O_CREATE|os.O_RDWR, fileMode)

				currentPos = 0
				currentSize = 0
			}

			newKeyDir[key] = record{
				fileId:    mergeFileName,
				valueSize: int64(len(value)),
				valuePos:  currentPos + staticFields*numberFieldSize + int64(len(key)),
				tstamp:    tstamp,
			}

			hintFileLine := buildHintFileLine(newKeyDir[key], key)
			n, _ := fmt.Fprintln(mergeFile, fileLine)
			fmt.Fprintln(hintFile, hintFileLine)
			currentPos += int64(n)
			currentSize += int64(n)
		} else {
			newKeyDir[key] = b.keyDir[key]
		}
	}

	b.keyDir = newKeyDir
	mergeFile.Close()
	hintFile.Close()

	for _, file := range oldFiles {
		if !strings.HasPrefix(file, ".") {
			os.Remove(path.Join(b.directoryPath, file))
		}
	}
	return nil
}

// Sync forces all pending writes to be written into disk.
// returns an error if ReadWrite permission is not set.
func (b *Bitcask) Sync() error {
	if b.config.writePermission == ReadOnly {
		return BitcaskError(WriteDenied)
	}

	err := b.activeFile.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

// Close flushes all pending writes into disk and closes the bitcask datastore.
func (b *Bitcask) Close() {
	if b.config.writePermission == ReadWrite {
		b.Sync()
		b.activeFile.file.Close()
		os.Remove(path.Join(b.directoryPath, b.lock))
	} else {
		os.Remove(path.Join(b.directoryPath, b.keyDirFile))
		os.Remove(path.Join(b.directoryPath, b.lock))
	}
	b = nil
}
