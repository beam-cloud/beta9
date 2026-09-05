package gatewayservices

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
)

// A zip archive is a sequence of local entries (header + data) followed by a
// central directory that records each entry's offset. Merging a delta into a
// base archive therefore never needs the entry bytes: the merged archive is
// the kept base entries, then the delta entries, byte for byte, followed by a
// central directory with updated offsets. S3 UploadPartCopy assembles that
// from ranges of the two existing objects, so a one-file change in a 200 MiB
// context costs the gateway a few MiB of traffic rather than 400 MiB.

const (
	zipLocalHeaderSignature    = 0x04034b50
	zipCentralHeaderSignature  = 0x02014b50
	zipEndOfCentralDirSig      = 0x06054b50
	zipCentralHeaderFixedLen   = 46
	zipEndOfCentralDirFixedLen = 22
	zipMaxCommentLen           = 0xffff
	zipCentralOffsetField      = 42 // relative offset of local header in a central directory record

	// s3MinPartSize is the smallest part S3 accepts except for the last one.
	s3MinPartSize = int64(5 << 20)
	// s3MaxCopyPartSize is the largest range UploadPartCopy accepts.
	s3MaxCopyPartSize = int64(5 << 30)
)

var errZipSpliceUnsupported = errors.New("zip archive layout not supported for splicing")

// zipEntry is one central directory record together with the byte span of
// its local entry (header, data and any data descriptor) in the archive.
type zipEntry struct {
	name   string
	offset int64  // local header offset
	end    int64  // offset of the next local header (or of the central directory)
	record []byte // raw central directory record
}

func (e zipEntry) size() int64 { return e.end - e.offset }

// zipDirectory is the parsed central directory of an archive.
type zipDirectory struct {
	entries  []zipEntry // in local header offset order
	cdOffset int64
}

// parseZipDirectory reads and parses the central directory of an archive of
// the given size. Only classic (non-zip64) archives are supported; anything
// else returns errZipSpliceUnsupported so the caller can merge the slow way.
func parseZipDirectory(ra io.ReaderAt, size int64) (*zipDirectory, error) {
	tailLen := int64(zipEndOfCentralDirFixedLen + zipMaxCommentLen)
	if tailLen > size {
		tailLen = size
	}
	tail := make([]byte, tailLen)
	if _, err := ra.ReadAt(tail, size-tailLen); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	eocd := -1
	for i := len(tail) - zipEndOfCentralDirFixedLen; i >= 0; i-- {
		if binary.LittleEndian.Uint32(tail[i:]) == zipEndOfCentralDirSig {
			commentLen := int(binary.LittleEndian.Uint16(tail[i+20:]))
			if i+zipEndOfCentralDirFixedLen+commentLen == len(tail) {
				eocd = i
				break
			}
		}
	}
	if eocd < 0 {
		return nil, fmt.Errorf("%w: end of central directory not found", errZipSpliceUnsupported)
	}
	rec := tail[eocd:]
	count := int(binary.LittleEndian.Uint16(rec[10:]))
	cdSize := int64(binary.LittleEndian.Uint32(rec[12:]))
	cdOffset := int64(binary.LittleEndian.Uint32(rec[16:]))
	if count == 0xffff || cdSize == 0xffffffff || cdOffset == 0xffffffff {
		return nil, fmt.Errorf("%w: zip64", errZipSpliceUnsupported)
	}
	if cdOffset+cdSize > size-tailLen+int64(eocd) {
		return nil, fmt.Errorf("%w: central directory overlaps its end record", errZipSpliceUnsupported)
	}

	cd := make([]byte, cdSize)
	if _, err := ra.ReadAt(cd, cdOffset); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	entries := make([]zipEntry, 0, count)
	for pos := 0; pos < len(cd); {
		if len(cd)-pos < zipCentralHeaderFixedLen || binary.LittleEndian.Uint32(cd[pos:]) != zipCentralHeaderSignature {
			return nil, fmt.Errorf("%w: malformed central directory", errZipSpliceUnsupported)
		}
		nameLen := int(binary.LittleEndian.Uint16(cd[pos+28:]))
		extraLen := int(binary.LittleEndian.Uint16(cd[pos+30:]))
		commentLen := int(binary.LittleEndian.Uint16(cd[pos+32:]))
		offset := int64(binary.LittleEndian.Uint32(cd[pos+zipCentralOffsetField:]))
		recLen := zipCentralHeaderFixedLen + nameLen + extraLen + commentLen
		if pos+recLen > len(cd) {
			return nil, fmt.Errorf("%w: truncated central directory record", errZipSpliceUnsupported)
		}
		if offset == 0xffffffff || binary.LittleEndian.Uint32(cd[pos+20:]) == 0xffffffff || binary.LittleEndian.Uint32(cd[pos+24:]) == 0xffffffff {
			return nil, fmt.Errorf("%w: zip64 entry", errZipSpliceUnsupported)
		}
		entries = append(entries, zipEntry{
			name:   string(cd[pos+zipCentralHeaderFixedLen : pos+zipCentralHeaderFixedLen+nameLen]),
			offset: offset,
			record: cd[pos : pos+recLen],
		})
		pos += recLen
	}
	if len(entries) != count {
		return nil, fmt.Errorf("%w: central directory lists %d entries, header says %d", errZipSpliceUnsupported, len(entries), count)
	}

	// Entries are usually written in offset order already; sort defensively
	// (insertion sort, the list is nearly sorted).
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].offset < entries[j-1].offset; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
	for i := range entries {
		if i+1 < len(entries) {
			entries[i].end = entries[i+1].offset
		} else {
			entries[i].end = cdOffset
		}
		if entries[i].offset < 0 || entries[i].end < entries[i].offset || (i == 0 && entries[i].offset != 0) {
			return nil, fmt.Errorf("%w: entries are not laid out sequentially", errZipSpliceUnsupported)
		}
	}
	return &zipDirectory{entries: entries, cdOffset: cdOffset}, nil
}

// spliceSource identifies which object a segment is copied from.
type spliceSource int

const (
	spliceFromBase spliceSource = iota
	spliceFromDelta
	spliceLiteral
)

// spliceSegment is a run of bytes of the merged archive: a range of the base
// or delta object, or literal bytes (the new central directory).
type spliceSegment struct {
	source spliceSource
	offset int64
	length int64
	data   []byte
}

// planZipSplice lays out the merged archive as segments and returns the plan
// with the merged size. Kept base entries come first in their original order,
// then all delta entries, then the rewritten central directory.
func planZipSplice(base, delta *zipDirectory, removedPaths []string) ([]spliceSegment, int64, error) {
	skip := make(map[string]struct{}, len(removedPaths)+len(delta.entries))
	for _, p := range removedPaths {
		skip[p] = struct{}{}
	}
	for _, e := range delta.entries {
		skip[e.name] = struct{}{}
	}

	var segments []spliceSegment
	var cd bytes.Buffer
	var pos int64
	count := 0

	appendRange := func(source spliceSource, offset, length int64) {
		if length == 0 {
			return
		}
		if n := len(segments); n > 0 && segments[n-1].source == source && segments[n-1].offset+segments[n-1].length == offset {
			segments[n-1].length += length
			return
		}
		segments = append(segments, spliceSegment{source: source, offset: offset, length: length})
	}
	appendEntry := func(source spliceSource, e zipEntry) error {
		if pos > 0xffffffff-e.size() {
			return fmt.Errorf("%w: merged archive needs zip64", errZipSpliceUnsupported)
		}
		rec := append([]byte(nil), e.record...)
		binary.LittleEndian.PutUint32(rec[zipCentralOffsetField:], uint32(pos))
		cd.Write(rec)
		appendRange(source, e.offset, e.size())
		pos += e.size()
		count++
		return nil
	}

	for _, e := range base.entries {
		if _, drop := skip[e.name]; drop {
			continue
		}
		if err := appendEntry(spliceFromBase, e); err != nil {
			return nil, 0, err
		}
	}
	for _, e := range delta.entries {
		if err := appendEntry(spliceFromDelta, e); err != nil {
			return nil, 0, err
		}
	}
	if count > 0xfffe || int64(cd.Len()) > 0xffffffff || pos+int64(cd.Len()) > 0xffffffff {
		return nil, 0, fmt.Errorf("%w: merged archive needs zip64", errZipSpliceUnsupported)
	}

	eocd := make([]byte, zipEndOfCentralDirFixedLen)
	binary.LittleEndian.PutUint32(eocd[0:], zipEndOfCentralDirSig)
	binary.LittleEndian.PutUint16(eocd[8:], uint16(count))
	binary.LittleEndian.PutUint16(eocd[10:], uint16(count))
	binary.LittleEndian.PutUint32(eocd[12:], uint32(cd.Len()))
	binary.LittleEndian.PutUint32(eocd[16:], uint32(pos))
	cd.Write(eocd)

	segments = append(segments, spliceSegment{source: spliceLiteral, length: int64(cd.Len()), data: cd.Bytes()})
	return segments, pos + int64(cd.Len()), nil
}

// spliceStore is the object store surface the splice needs; it is satisfied
// by S3 and by an in-memory implementation in tests.
type spliceStore interface {
	ObjectSize(ctx context.Context, key string) (int64, error)
	ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	BeginUpload(ctx context.Context, key string, metadata map[string]string) (uploadID string, err error)
	CopyPart(ctx context.Context, key, uploadID string, partNumber int32, sourceKey string, offset, length int64) (etag string, err error)
	PutPart(ctx context.Context, key, uploadID string, partNumber int32, data []byte) (etag string, err error)
	CompleteUpload(ctx context.Context, key, uploadID string, etags []string) error
	AbortUpload(ctx context.Context, key, uploadID string) error
}

// rangeReaderAt adapts ReadRange to io.ReaderAt for the directory parser.
type rangeReaderAt struct {
	ctx   context.Context
	store spliceStore
	key   string
	size  int64
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		// An archive with no entries has an empty central directory; a
		// zero-length HTTP range (bytes=0--1) is invalid and would fail the
		// splice for every delta that only removes files.
		return 0, nil
	}
	if off >= r.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if off+n > r.size {
		n = r.size - off
	}
	data, err := r.store.ReadRange(r.ctx, r.key, off, n)
	if err != nil {
		return 0, err
	}
	copy(p, data)
	if int64(len(data)) < int64(len(p)) {
		return len(data), io.EOF
	}
	return len(data), nil
}

// spliceZipObjects writes targetKey as the merge of baseKey and deltaKey
// using server-side range copies, moving only small ranges and the central
// directory through this process. Returns the merged size.
func spliceZipObjects(ctx context.Context, store spliceStore, baseKey, deltaKey, targetKey string, removedPaths []string, metadata map[string]string, minPart int64) (int64, error) {
	baseSize, err := store.ObjectSize(ctx, baseKey)
	if err != nil {
		return 0, fmt.Errorf("base size: %w", err)
	}
	deltaSize, err := store.ObjectSize(ctx, deltaKey)
	if err != nil {
		return 0, fmt.Errorf("delta size: %w", err)
	}
	base, err := parseZipDirectory(&rangeReaderAt{ctx, store, baseKey, baseSize}, baseSize)
	if err != nil {
		return 0, fmt.Errorf("base archive: %w", err)
	}
	delta, err := parseZipDirectory(&rangeReaderAt{ctx, store, deltaKey, deltaSize}, deltaSize)
	if err != nil {
		return 0, fmt.Errorf("delta archive: %w", err)
	}
	segments, size, err := planZipSplice(base, delta, removedPaths)
	if err != nil {
		return 0, err
	}

	sourceKey := func(s spliceSource) string {
		if s == spliceFromBase {
			return baseKey
		}
		return deltaKey
	}

	parts := planSpliceParts(segments, minPart, spliceCopyPartSize)

	uploadID, err := store.BeginUpload(ctx, targetKey, metadata)
	if err != nil {
		return 0, fmt.Errorf("begin upload: %w", err)
	}
	etags := make([]string, len(parts))
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(spliceConcurrency)
	for i, part := range parts {
		i, part := i, part
		group.Go(func() error {
			partNumber := int32(i + 1)
			if part.copyFrom != nil {
				etag, err := store.CopyPart(gctx, targetKey, uploadID, partNumber, sourceKey(*part.copyFrom), part.offset, part.length)
				if err != nil {
					return fmt.Errorf("copy part %d: %w", partNumber, err)
				}
				etags[i] = etag
				return nil
			}
			var data []byte
			for _, piece := range part.pieces {
				if piece.source == spliceLiteral {
					data = append(data, piece.data...)
					continue
				}
				chunk, err := store.ReadRange(gctx, sourceKey(piece.source), piece.offset, piece.length)
				if err != nil {
					return fmt.Errorf("read range for part %d: %w", partNumber, err)
				}
				data = append(data, chunk...)
			}
			etag, err := store.PutPart(gctx, targetKey, uploadID, partNumber, data)
			if err != nil {
				return fmt.Errorf("put part %d: %w", partNumber, err)
			}
			etags[i] = etag
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		abortSpliceUpload(ctx, store, targetKey, uploadID)
		return 0, err
	}
	if err := store.CompleteUpload(ctx, targetKey, uploadID, etags); err != nil {
		abortSpliceUpload(ctx, store, targetKey, uploadID)
		return 0, fmt.Errorf("complete upload: %w", err)
	}
	return size, nil
}

// abortSpliceUpload discards a failed multipart upload. The failure may be
// the request context being canceled, in which case an abort on that context
// never reaches the store and the parts linger; run it detached and bounded.
func abortSpliceUpload(ctx context.Context, store spliceStore, key, uploadID string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_ = store.AbortUpload(ctx, key, uploadID)
}

const (
	// spliceCopyPartSize bounds server-side copy parts so a large archive is
	// copied by many concurrent requests rather than one long one: on Wasabi
	// a single UploadPartCopy runs at roughly 16 MB/s regardless of size, so
	// small parts with wide fanout are what make the splice fast.
	spliceCopyPartSize = int64(8 << 20)
	spliceConcurrency  = 32
)

// splicePart is one multipart-upload part: either a single server-side copy
// range or a literal part assembled from small ranges and generated bytes.
type splicePart struct {
	copyFrom *spliceSource
	offset   int64
	length   int64
	pieces   []spliceSegment
}

// planSpliceParts groups segments into multipart parts. Every part but the
// last must be at least minPart: large ranges become copy parts of at most
// copyPart bytes; small ranges are gathered into literal parts, borrowing the
// head of the following range when a literal part would otherwise be short.
func planSpliceParts(segments []spliceSegment, minPart, copyPart int64) []splicePart {
	var parts []splicePart
	var literal []spliceSegment
	var literalLen int64

	flushLiteral := func() {
		if literalLen == 0 {
			return
		}
		parts = append(parts, splicePart{pieces: literal, length: literalLen})
		literal, literalLen = nil, 0
	}
	addLiteral := func(seg spliceSegment) {
		literal = append(literal, seg)
		literalLen += seg.length
	}

	for _, seg := range segments {
		if seg.source == spliceLiteral {
			addLiteral(seg)
			if literalLen >= minPart {
				flushLiteral()
			}
			continue
		}

		offset, remaining := seg.offset, seg.length
		if literalLen > 0 && literalLen < minPart {
			borrow := minPart - literalLen
			if borrow > remaining {
				borrow = remaining
			}
			addLiteral(spliceSegment{source: seg.source, offset: offset, length: borrow})
			offset += borrow
			remaining -= borrow
		}
		if remaining > 0 && remaining < minPart {
			addLiteral(spliceSegment{source: seg.source, offset: offset, length: remaining})
			remaining = 0
		}
		if literalLen >= minPart {
			flushLiteral()
		}
		for remaining > 0 {
			n := remaining
			if n > copyPart {
				n = copyPart
			}
			if remaining-n > 0 && remaining-n < minPart {
				n = remaining - minPart // keep the tail copyable
			}
			source := seg.source
			parts = append(parts, splicePart{copyFrom: &source, offset: offset, length: n})
			offset += n
			remaining -= n
		}
	}
	flushLiteral()
	return parts
}

// s3SpliceStore implements spliceStore on an S3 bucket.
type s3SpliceStore struct {
	client *s3.Client
	bucket string
}

func (s *s3SpliceStore) ObjectSize(ctx context.Context, key string) (int64, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return 0, err
	}
	return aws.ToInt64(out.ContentLength), nil
}

func (s *s3SpliceStore) ReadRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("range read of %s returned %d bytes, want %d", key, len(data), length)
	}
	return data, nil
}

func (s *s3SpliceStore) BeginUpload(ctx context.Context, key string, metadata map[string]string) (string, error) {
	out, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		Metadata: metadata,
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.UploadId), nil
}

func (s *s3SpliceStore) CopyPart(ctx context.Context, key, uploadID string, partNumber int32, sourceKey string, offset, length int64) (string, error) {
	out, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		PartNumber:      aws.Int32(partNumber),
		CopySource:      aws.String(s.bucket + "/" + sourceKey),
		CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.CopyPartResult.ETag), nil
}

func (s *s3SpliceStore) PutPart(ctx context.Context, key, uploadID string, partNumber int32, data []byte) (string, error) {
	out, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(partNumber),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

func (s *s3SpliceStore) CompleteUpload(ctx context.Context, key, uploadID string, etags []string) error {
	parts := make([]s3types.CompletedPart, len(etags))
	for i, etag := range etags {
		parts[i] = s3types.CompletedPart{ETag: aws.String(etag), PartNumber: aws.Int32(int32(i + 1))}
	}
	_, err := s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
	})
	return err
}

func (s *s3SpliceStore) AbortUpload(ctx context.Context, key, uploadID string) error {
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return err
}
