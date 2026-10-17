package storage

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"sort"
	"sync"
)

type classicFileIo struct {
	mu      sync.Mutex
	writers map[string]*classicCachedFile
	closed  bool
}

// A cached writable handle. Each write, and each borrowed reader until it is closed, holds mu for
// reading while it uses f, and closing takes mu for writing, so closing a cached handle waits for the
// users in flight instead of failing them with fs.ErrClosed. New users only reach it through
// classicFileIo.mu, which is held while closing, so a pending close can't starve behind new readers.
type classicCachedFile struct {
	mu sync.RWMutex
	f  *os.File
}

func (me *classicCachedFile) close() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	return me.f.Close()
}

func newClassicFileIo() fileIo {
	return &classicFileIo{}
}

func (me *classicFileIo) Close() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	_, _, err := me.closeWritersLocked()
	me.closed = true
	return err
}

func (me *classicFileIo) closeWriters() (closedPaths []string, remaining int, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return nil, 0, err
	}
	return me.closeWritersLocked()
}

func (me *classicFileIo) rename(from, to string) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return err
	}
	// Restoring a part file after a failed piece hash renames from a complete file that usually
	// doesn't exist. Keep the cached writers in that case: there is nothing to release.
	if _, err := os.Lstat(from); err != nil {
		return err
	}
	err := me.closeWriterLocked(from)
	err = errors.Join(err, me.closeWriterLocked(to))
	err = errors.Join(err, os.Rename(from, to))
	return err
}

func (me *classicFileIo) flush(name string, offset, nbytes int64) error {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return err
	}
	if cached, ok := me.writers[name]; ok {
		return cached.f.Sync()
	}
	return fsync(name)
}

func (me *classicFileIo) openForSharedRead(name string) (sharableReader, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return nil, err
	}
	return os.Open(name)
}

func (me *classicFileIo) openForRead(name string) (fileReader, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if err := me.closedErr(); err != nil {
		return nil, err
	}
	if cached, ok := me.writers[name]; ok {
		cached.mu.RLock()
		return &classicBorrowedFileReader{cached: cached, f: cached.f}, nil
	}
	f, err := os.Open(name)
	return classicFileReader{f}, err
}

func (me *classicFileIo) openForWrite(p string, size int64) (f fileWriter, err error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if _, err := me.cachedWriterLocked(p); err != nil {
		return nil, err
	}
	return classicFileWriter{io: me, name: p}, nil
}

// Returns the cached writer for p, opening it if it was closed since, locked for use.
func (me *classicFileIo) useWriter(p string) (*classicCachedFile, error) {
	me.mu.Lock()
	defer me.mu.Unlock()
	cached, err := me.cachedWriterLocked(p)
	if err != nil {
		return nil, err
	}
	cached.mu.RLock()
	return cached, nil
}

func (me *classicFileIo) cachedWriterLocked(p string) (*classicCachedFile, error) {
	if err := me.closedErr(); err != nil {
		return nil, err
	}
	if cached, ok := me.writers[p]; ok {
		return cached, nil
	}
	f, err := openFileExtra(p, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	if me.writers == nil {
		me.writers = make(map[string]*classicCachedFile)
	}
	cached := &classicCachedFile{f: f}
	me.writers[p] = cached
	return cached, nil
}

func (me *classicFileIo) closeWriterLocked(name string) error {
	if me.writers == nil {
		return nil
	}
	cached, ok := me.writers[name]
	if !ok {
		return nil
	}
	delete(me.writers, name)
	return cached.close()
}

func (me *classicFileIo) closeWritersLocked() (closedPaths []string, remaining int, err error) {
	if me.writers == nil {
		return nil, 0, nil
	}
	closedPaths = make([]string, 0, len(me.writers))
	for name := range me.writers {
		closedPaths = append(closedPaths, name)
	}
	sort.Strings(closedPaths)
	for _, name := range closedPaths {
		cached := me.writers[name]
		delete(me.writers, name)
		err = errors.Join(err, cached.close())
	}
	return closedPaths, len(me.writers), err
}

func (me *classicFileIo) closedErr() error {
	if me.closed {
		return fs.ErrClosed
	}
	return nil
}

// Looks up the cached handle for each write, so a handle closed since the writer was opened (by a
// rename or closeWriters) is reopened instead of failing the write.
type classicFileWriter struct {
	io   *classicFileIo
	name string
}

func (me classicFileWriter) WriteAt(p []byte, off int64) (int, error) {
	cached, err := me.io.useWriter(me.name)
	if err != nil {
		return 0, err
	}
	defer cached.mu.RUnlock()
	return cached.f.WriteAt(p, off)
}

func (classicFileWriter) Close() error {
	return nil
}

type classicFileReader struct {
	*os.File
}

func (c classicFileReader) writeToN(w io.Writer, n int64) (written int64, err error) {
	lw := limitWriter{
		rem: n,
		w:   w,
	}
	return c.File.WriteTo(&lw)
}

func (c classicFileReader) seekDataOrEof(offset int64) (ret int64, err error) {
	ret, err = seekData(c.File, offset)
	if err == io.EOF {
		ret, err = c.File.Seek(0, io.SeekEnd)
	}
	return
}

// Reuses the open writer handle to avoid an extra open during piece hashing.
// This is intentionally only used for openForRead, not shared readers, so the
// borrowed handle lifetime stays scoped to storage-internal operations.
type classicBorrowedFileReader struct {
	cached *classicCachedFile
	f      *os.File
	pos    int64
}

func (c *classicBorrowedFileReader) Close() error {
	if c.cached != nil {
		c.cached.mu.RUnlock()
		c.cached = nil
	}
	return nil
}

func (c *classicBorrowedFileReader) Read(p []byte) (n int, err error) {
	n, err = c.f.ReadAt(p, c.pos)
	c.pos += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return
}

func (c *classicBorrowedFileReader) writeToN(w io.Writer, n int64) (written int64, err error) {
	written, err = io.Copy(w, io.NewSectionReader(c.f, c.pos, n))
	c.pos += written
	return
}

func (c *classicBorrowedFileReader) seekDataOrEof(offset int64) (ret int64, err error) {
	ret, err = seekData(c.f, offset)
	if err == io.EOF {
		var fi os.FileInfo
		fi, err = c.f.Stat()
		if err != nil {
			return c.pos, err
		}
		ret = fi.Size()
		err = nil
	}
	if err != nil {
		return c.pos, err
	}
	c.pos = ret
	return
}
