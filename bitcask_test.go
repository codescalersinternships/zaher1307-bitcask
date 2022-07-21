package bitcask

import (
	"fmt"
	"os"
	"path"
	"testing"
)

var testBitcaskPath = path.Join("testing_dir")
var testKeyDirPath = path.Join("testing_dir", "keydir")

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
        _, err := Open("no open dir")
        if err == nil {
            t.Fatal("Expected Error since path cannot be openned")
        }
        os.RemoveAll("no open dir")
    })

}
