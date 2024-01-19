package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type HttpRoot struct {
	fs.Inode
	url string
}

type httpFile struct {
	fs.Inode
	url    string
	size   int64
	client *http.Client
}

var _ = (fs.NodeOpener)((*httpFile)(nil))
var _ = (fs.NodeGetattrer)((*httpFile)(nil))

func (hf *httpFile) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 07777
	out.Nlink = 1
	out.Mtime = uint64(time.Now().Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	out.Size = uint64(hf.size)
	const bs = 128 * 1024 // 128kb -> max fuse size aka 32 pages @ 4kb
	out.Blksize = bs
	out.Blocks = (out.Size + bs - 1) / bs
	return 0
}

func (hf *httpFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, 0 // dont need a file handle
}

func (hf *httpFile) Read(ctx context.Context, f fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	var req, _ = http.NewRequest("GET", hf.url, nil)

	end := int64(off) + int64(len(dest))
	if end > hf.size {
		end = hf.size
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end-1))

	var resp, err = hf.client.Do(req)

	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 206 {
		log.Fatalf("status code was not partial content: %d", resp.StatusCode)
	}

	bytes, err := io.ReadAll(resp.Body)

	if err != nil {
		log.Fatal(err)
	}

	return fuse.ReadResultData(bytes), 0
}

func (r *HttpRoot) OnAdd(ctx context.Context) {

	httpfile := mountFile(r.url)

	ch := r.NewPersistentInode(ctx, httpfile, fs.StableAttr{Ino: 2})
	r.AddChild("test.iso", ch, false)
}

func mountFile(url string) *httpFile {
	outfile := new(httpFile)
	outfile.url = url
	outfile.client = new(http.Client)

	outfile.client.Transport = &http.Transport{
		MaxIdleConns:          100,
		MaxConnsPerHost:       100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       15 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		DisableKeepAlives:     false,
	}

	var rsp, err = outfile.client.Get(url)

	if err != nil {
		log.Fatal(err)
	}

	rsp.Body.Close()

	if rsp.Header.Get("Accept-Ranges") != "bytes" {
		log.Fatal("server does not accept range bytes")
	}

	contentLen, err := strconv.ParseInt(rsp.Header.Get("Content-Length"), 10, 64)

	if err != nil {
		log.Fatal(err)
	}

	outfile.size = contentLen

	return outfile
}

func (r *HttpRoot) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

var _ = (fs.NodeGetattrer)((*HttpRoot)(nil))
var _ = (fs.NodeOnAdder)((*HttpRoot)(nil))

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	url := flag.String("url", "", "which file to mount")
	mountpoint := flag.String("mount", "", "set mount point")
	flag.Parse()

	if *url == "" || *mountpoint == "" {
		log.Fatal("Usage:\n  go-httpmount --url <file-url> --mount <path> [--debug]")

	}

	opts := &fs.Options{}
	opts.Name = "go-httpmount"
	opts.Debug = *debug
	opts.ExplicitDataCacheControl = true
	opts.MaxReadAhead = 1024 * 128
	server, err := fs.Mount(*mountpoint, &HttpRoot{url: *url}, opts)

	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	server.Wait()
}
