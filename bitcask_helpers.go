package bitcask

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

func (b *Bitcask) createActiveFile() error {
	fileName := strconv.FormatInt(time.Now().UnixMicro(), 10)

	fileFlags := os.O_CREATE | os.O_RDWR
	if b.config.syncOption == SyncOnPut {
		fileFlags |= os.O_SYNC
	}

	activeFile, err := os.OpenFile(path.Join(b.directoryPath, fileName), fileFlags, fileMode)
	if err != nil {
		return err
	}

	b.activeFile.file = activeFile
	b.activeFile.fileName = fileName
	b.activeFile.currentPos = 0
	b.activeFile.currentSize = 0
	return nil
}

func (b *Bitcask) buildKeyDir() {
	if b.config.writePermission == ReadOnly && b.lockCheck() == reader {
		keyDirData, _ := os.ReadFile(path.Join(b.directoryPath, b.keyDirFileCheck()))

		b.keyDir = make(map[string]record)
		keyDirScanner := bufio.NewScanner(strings.NewReader(string(keyDirData)))

		for keyDirScanner.Scan() {
			line := keyDirScanner.Text()

			key, fileId, valueSize, valuePos, tstamp := extractKeyDirFileLine(line)

			b.keyDir[key] = record{
				fileId:    fileId,
				valueSize: valueSize,
				valuePos:  valuePos,
				tstamp:    tstamp,
			}
		}
	} else {
		var fileNames []string
		hintFilesMap := make(map[string]string)
		bitcaskDir, _ := os.Open(b.directoryPath)
		files, _ := bitcaskDir.Readdir(0)

		for _, file := range files {
			name := file.Name()
			if strings.HasPrefix(name, hintFilePrefix) {
				hintFilesMap[strings.Trim(name, hintFilePrefix)] = name
				fileNames = append(fileNames, strings.Trim(name, hintFilePrefix))
			} else {
				fileNames = append(fileNames, name)
			}
		}

		for _, name := range fileNames {
			if hint, isExist := hintFilesMap[name]; isExist {
				b.extractHintFile(hint)
			} else {
				var currentPos int64 = 0
				fileData, _ := os.ReadFile(path.Join(b.directoryPath, name))
				fileScanner := bufio.NewScanner(strings.NewReader(string(fileData)))
				for fileScanner.Scan() {
					line := fileScanner.Text()
					key, _, tstamp, keySize, valueSize := extractFileLine(line)
					b.keyDir[key] = record{
						fileId:    name,
						valueSize: valueSize,
						valuePos:  currentPos + staticFields*numberFieldSize + keySize,
						tstamp:    tstamp,
					}
					currentPos += int64(len(line) + 1)
				}
			}
		}
	}
}

func (b *Bitcask) writeToActiveFile(line string) (int64, error) {
	if int64(len(line))+b.activeFile.currentSize > maxFileSize {
		err := b.createActiveFile()
		if err != nil {
			return 0, err
		}
	}

	n, err := b.activeFile.file.Write([]byte(fmt.Sprintln(line)))
	if err != nil {
		return 0, err
	}
	return int64(n), nil
}

func compressFileLine(key string, value string, tstamp int64) []byte {
	tstampStr := padWithZero(tstamp)
	keySize := padWithZero(int64(len([]byte(key))))
	valueSize := padWithZero(int64(len([]byte(value))))
	return []byte(tstampStr + keySize + valueSize + string(key) + value)
}

func extractFileLine(line string) (string, string, int64, int64, int64) {
	tstamp, _ := strconv.ParseInt(line[0:19], 10, 64)
	keySize, _ := strconv.ParseInt(line[19:38], 10, 64)
	valueSize, _ := strconv.ParseInt(line[38:57], 10, 64)
	key := line[57 : 57+keySize]
	value := line[57+keySize:]
	return key, value, tstamp, keySize, valueSize
}

func (b *Bitcask) buildKeyDirFile() {
	keyDirFileName := keyDirFilePrefix + strconv.FormatInt(time.Now().UnixMicro(), 10)
	b.keyDirFile = keyDirFileName
	keyDirFile, _ := os.Create(path.Join(b.directoryPath, keyDirFileName))
	for key, recValue := range b.keyDir {
		fileId, _ := strconv.ParseInt(recValue.fileId, 10, 64)
		fileIdStr := padWithZero(fileId)
		valueSizeStr := padWithZero(recValue.valueSize)
		valuePosStr := padWithZero(recValue.valuePos)
		tstampStr := padWithZero(recValue.tstamp)
		keySizeStr := padWithZero(int64(len(key)))

		line := fileIdStr + valueSizeStr + valuePosStr + tstampStr + keySizeStr + key
		fmt.Fprintln(keyDirFile, line)
	}
}

func extractKeyDirFileLine(line string) (string, string, int64, int64, int64) {
	fileId, _ := strconv.ParseInt(line[0:19], 10, 64)
	valueSize, _ := strconv.ParseInt(line[19:38], 10, 64)
	valuePos, _ := strconv.ParseInt(line[38:57], 10, 64)
	tstamp, _ := strconv.ParseInt(line[57:76], 10, 64)
	keySize, _ := strconv.ParseInt(line[76:95], 10, 64)
	key := line[95 : 95+keySize]
	return key, strconv.FormatInt(fileId, 10), valueSize, valuePos, tstamp
}

func buildHintFileLine(recValue record, key string) string {
	tstamp := padWithZero(recValue.tstamp)
	keySize := padWithZero(int64(len(key)))
	valueSize := padWithZero(recValue.valueSize)
	valuePos := padWithZero(recValue.valuePos)
	return tstamp + keySize + valueSize + valuePos + key
}

func (b *Bitcask) extractHintFile(hintName string) {
	hintFileData, _ := os.ReadFile(path.Join(b.directoryPath, hintName))
	hintFileScanner := bufio.NewScanner(strings.NewReader(string(hintFileData)))

	fileId := strings.Trim(hintName, hintFilePrefix)

	for hintFileScanner.Scan() {
		line := hintFileScanner.Text()
		tstamp, _ := strconv.ParseInt(line[0:19], 10, 64)
		keySize, _ := strconv.ParseInt(line[19:38], 10, 64)
		valueSize, _ := strconv.ParseInt(line[38:57], 10, 64)
		valuePos, _ := strconv.ParseInt(line[57:76], 10, 64)
		key := line[76 : 76+keySize]

		b.keyDir[key] = record{
			fileId:    fileId,
			valueSize: valueSize,
			valuePos:  valuePos,
			tstamp:    tstamp,
		}
	}
}

func (b *Bitcask) lockCheck() processAccess {
	bitcaskDir, _ := os.Open(b.directoryPath)

	files, _ := bitcaskDir.Readdir(0)

	for _, file := range files {
		if strings.HasPrefix(file.Name(), readLock) {
			return reader
		} else if strings.HasPrefix(file.Name(), writeLock) {
			return writer
		}
	}
	return noProcess
}

func (b *Bitcask) keyDirFileCheck() string {
	var fileName string
	bitcaskDir, _ := os.Open(b.directoryPath)

	files, _ := bitcaskDir.Readdir(0)

	for _, file := range files {
		if strings.HasPrefix(file.Name(), keyDirFilePrefix) {
			fileName = file.Name()
			break
		}
	}
	return fileName
}

func padWithZero(val int64) string {
	return fmt.Sprintf("%019d", val)
}
