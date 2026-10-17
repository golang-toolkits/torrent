package storage

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-quicktest/qt"
)

func TestDefaultFileIoDefaultsToMmapWithoutEnv(t *testing.T) {
	t.Setenv("TORRENT_STORAGE_DEFAULT_FILE_IO", "")
	qt.Assert(t, qt.IsNil(os.Unsetenv("TORRENT_STORAGE_DEFAULT_FILE_IO")))
	ioImpl := defaultFileIo()
	t.Cleanup(func() { _ = ioImpl.Close() })
	_, ok := ioImpl.(*mmapFileIo)
	qt.Assert(t, qt.IsTrue(ok))
}

func TestDefaultFileIoReadsEnvironmentLazily(t *testing.T) {
	t.Setenv("TORRENT_STORAGE_DEFAULT_FILE_IO", "classic")
	ioImpl := defaultFileIo()
	t.Cleanup(func() { _ = ioImpl.Close() })
	_, ok := ioImpl.(*classicFileIo)
	qt.Assert(t, qt.IsTrue(ok))

	t.Setenv("TORRENT_STORAGE_DEFAULT_FILE_IO", "mmap")
	ioImpl = defaultFileIo()
	t.Cleanup(func() { _ = ioImpl.Close() })
	_, ok = ioImpl.(*mmapFileIo)
	qt.Assert(t, qt.IsTrue(ok))
}

func TestClassicFileIoRenameClosesCachedWriter(t *testing.T) {
	tempDir := t.TempDir()
	oldPath := filepath.Join(tempDir, "old.bin")
	newPath := filepath.Join(tempDir, "new.bin")

	ioImpl, ok := newClassicFileIo().(*classicFileIo)
	qt.Assert(t, qt.IsTrue(ok))
	t.Cleanup(func() { _ = ioImpl.Close() })

	writer, err := ioImpl.openForWrite(oldPath, 0)
	qt.Assert(t, qt.IsNil(err))
	_, err = writer.WriteAt([]byte("hello"), 0)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.IsNil(writer.Close()))

	qt.Assert(t, qt.IsNil(ioImpl.rename(oldPath, newPath)))

	data, err := os.ReadFile(newPath)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.DeepEquals(data, []byte("hello")))
}

func TestClassicFileIoOpenForReadBorrowsWriterHandle(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "data.bin")

	ioImpl, ok := newClassicFileIo().(*classicFileIo)
	qt.Assert(t, qt.IsTrue(ok))
	t.Cleanup(func() { _ = ioImpl.Close() })

	writer, err := ioImpl.openForWrite(filePath, 0)
	qt.Assert(t, qt.IsNil(err))
	_, err = writer.WriteAt([]byte("hello"), 0)
	qt.Assert(t, qt.IsNil(err))

	reader, err := ioImpl.openForRead(filePath)
	qt.Assert(t, qt.IsNil(err))
	defer reader.Close()

	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(n, 5))
	qt.Assert(t, qt.DeepEquals(buf, []byte("hello")))
}
func TestClassicFileIoCloseWritersClearsCachedHandles(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "data.bin")

	ioImpl, ok := newClassicFileIo().(*classicFileIo)
	qt.Assert(t, qt.IsTrue(ok))
	t.Cleanup(func() { _ = ioImpl.Close() })

	writer, err := ioImpl.openForWrite(filePath, 0)
	qt.Assert(t, qt.IsNil(err))
	_, err = writer.WriteAt([]byte("hello"), 0)
	qt.Assert(t, qt.IsNil(err))

	closedPaths, remaining, err := ioImpl.closeWriters()
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.DeepEquals(closedPaths, []string{filePath}))
	qt.Assert(t, qt.Equals(remaining, 0))
	qt.Assert(t, qt.HasLen(ioImpl.writers, 0))
}

// Piece hash failures rename a file that is usually missing, and completion can close every cached
// writer. Neither may fail writes to other pieces of the same file that are in flight.
func TestClassicFileIoWritesSurviveConcurrentWriterCloses(t *testing.T) {
	tempDir := t.TempDir()
	partPath := filepath.Join(tempDir, "data.bin.part")
	completePath := filepath.Join(tempDir, "data.bin")

	ioImpl := newClassicFileIo()
	t.Cleanup(func() { _ = ioImpl.Close() })

	const (
		writers     = 8
		chunkSize   = 4 << 10
		chunksPerGo = 200
	)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	closerDone := make(chan struct{})
	go func() {
		defer close(closerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			err := ioImpl.rename(completePath, partPath)
			if !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("renaming missing file: %v", err)
			}
			_, _, err = ioImpl.closeWriters()
			if err != nil {
				t.Errorf("closing writers: %v", err)
			}
		}
	}()
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chunk := bytes.Repeat([]byte{byte(w + 1)}, chunkSize)
			for i := range chunksPerGo {
				off := int64(i*writers+w) * chunkSize
				f, err := ioImpl.openForWrite(partPath, 0)
				if err != nil {
					t.Errorf("opening for write: %v", err)
					return
				}
				_, err = f.WriteAt(chunk, off)
				closeErr := f.Close()
				if err != nil || closeErr != nil {
					t.Errorf("writing chunk %d: %v, %v", i, err, closeErr)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-closerDone
	qt.Assert(t, qt.IsNil(ioImpl.Close()))

	data, err := os.ReadFile(partPath)
	qt.Assert(t, qt.IsNil(err))
	qt.Assert(t, qt.Equals(len(data), writers*chunksPerGo*chunkSize))
	for i := range writers * chunksPerGo {
		want := byte(i%writers + 1)
		chunk := data[i*chunkSize : (i+1)*chunkSize]
		qt.Assert(t, qt.Equals(bytes.Count(chunk, []byte{want}), chunkSize))
	}
}
