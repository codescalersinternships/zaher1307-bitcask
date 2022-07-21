package bitcask

import (
	"fmt"
	"os"
	"path"
	"testing"
)

var testBitcaskPath = path.Join("testing_dir")
var testKeyDirPath = path.Join("testing_dir", "keydir")
var testFilePath = path.Join("testing_dir", "testfile")
var testFileLine = padWithZero(34567) + padWithZero(5) + padWithZero(10) + "key12" + "value12345"

func TestOpen(t *testing.T) {

    t.Run("open new bitcask with read and write permission", func(t *testing.T) {

        Open(testBitcaskPath, ReadWrite)
        
        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)

    })

    t.Run("open new bitcask with sync_on_put option", func(t *testing.T) {

        Open(testBitcaskPath, ReadWrite, SyncOnPut)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)

    })

    t.Run("open new bitcask with default options", func(t *testing.T) {

        Open(testBitcaskPath)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)

    })

    t.Run("open existing bitcask with read and write permission", func(t *testing.T) {

        Open(testBitcaskPath, ReadWrite)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

        Open(testBitcaskPath, ReadWrite)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)

    })

    t.Run("open existing bitcask with sync on put option", func(t *testing.T) {

        Open(testBitcaskPath, SyncOnPut)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

        Open(testBitcaskPath, SyncOnPut)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)

    })

    t.Run("open existing bitcask with default options", func(t *testing.T) {

        Open(testBitcaskPath)

        testKeyDir, _ := os.Create(testKeyDirPath)
        fmt.Fprintln(testKeyDir, "key 1 50 0 3")

        Open(testBitcaskPath)

        if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
            t.Errorf("Expected to find directory: %q", testBitcaskPath)
        }
        os.RemoveAll(testBitcaskPath)

    })

    t.Run("open bitcask failed", func(t *testing.T) {

        os.MkdirAll(path.Join("no open dir"), 000)

        want := "no open dir: cannot open this directory"
        _, err := Open("no open dir")

        assertError(t, err, want)

        os.RemoveAll("no open dir")    

    })

}

func TestGet(t *testing.T) {

    t.Run("existing value from file", func(t *testing.T) {
        
        os.MkdirAll(testBitcaskPath, 0700)
        file, _ := os.Create(testFilePath)
        file.Write([]byte(testFileLine))
        b, _ := Open(testBitcaskPath)

        b.keyDir["key12"] = record{
        	fileId:    "testfile",
        	valueSize: 10,
        	valuePos:  62,
        	tstamp:    34567,
        	isPending: false,
        }

        got, _ := b.Get("key12")
        want := "value12345"

        if got != want {
            t.Errorf("got:\n%q\nwant:\n%q", got,want)
        }

        os.RemoveAll(testBitcaskPath)

    })

    t.Run("existing value from pending list", func(t *testing.T) {

        b, _ := Open(testBitcaskPath, ReadWrite)

        b.keyDir["key12"] = record{
            fileId:    "testfile",
            valueSize: 10,
            valuePos:  62,
            tstamp:    34567,
            isPending: true,
        }

        b.pendingWrites["key12"] = fileLine(composeFileLine(key("key12"), "value12345"))

        got, _ := b.Get("key12")
        want := "value12345"

        os.RemoveAll(testBitcaskPath)
        assertString(t, got, want)

    })

    t.Run("not existing value", func(t *testing.T) {

        b, _ := Open(testBitcaskPath)

        want := "unknown key: key does not exist"
        _, err := b.Get("unknown key")

        os.RemoveAll(testBitcaskPath)
        assertError(t, err, want)

    })

}

func assertError(t testing.TB, err error, want string) {
    t.Helper()
    if err == nil {
        t.Fatalf("Expected Error %q", want)
    }
    assertString(t, err.Error(), want)
}

func assertString(t testing.TB, got, want string) {
    t.Helper()
    if got != want {
        t.Errorf("got:\n%q\nwant:\n%q", got, want)
    }
}
