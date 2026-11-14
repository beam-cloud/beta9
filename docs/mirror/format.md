# Mirror store format, version 1

A **mirror** is a named collection of local files kept in the workspace
bucket. A **generation** is one committed version of it. This document
specifies what a writer puts in the bucket and what a reader may rely on.
The Go implementation in `pkg/mirror` is the reference; `pkg/mirror/testdata`
holds the vectors every implementation must reproduce.

## Objects

Everything a mirror stores is an **object**: an immutable byte string named by
the lowercase hex SHA-256 of its stored bytes, at

    mirrors/<mirror id>/objects/<sha256>

in the workspace bucket. The mirror id is the UUID the gateway assigned when
the mirror was created. There are two uses of an object, and nothing in the
store tells them apart:

- **Content objects** hold file bytes. A manifest refers to byte ranges of
  them. They are at most 64 MiB.
- **Manifests** describe one generation each. The gateway commits a manifest
  whose stored bytes and decoded body are each at most 256 MiB, which is
  about three million files.

Objects are never modified. A generation is the set of objects its manifest
names plus the manifest itself, so two generations share every object they
have in common, and deleting a mirror is deleting its prefix. Nothing is
shared between mirrors.

## Extents

A file's content is a sequence of **extents**. An extent is a byte range of
one object:

| field | meaning |
| --- | --- |
| `object` | index into the manifest's object table |
| `offset` | where the extent starts in the object |
| `stored_len` | how many bytes it occupies there |
| `raw_len` | how many file bytes it holds |

If `stored_len == raw_len` the range is the file bytes. If
`stored_len < raw_len` the range is zstd data that decompresses to exactly
`raw_len` bytes. A writer stores one frame; a reader does not check that
there is only one. `raw_len` is at most 16 MiB, so a reader never
decompresses more than that to serve a read. `stored_len > raw_len` is
invalid: a writer stores an extent uncompressed when compression does not
shrink it.

How a writer cuts files into extents and groups extents into objects is its
own policy and is invisible to readers. The reference policy is under
[Writer policy](#writer-policy).

## Manifest

All integers are little-endian. A stored manifest is a 24-byte envelope
followed by the body:

| offset | size | field |
| --- | --- | --- |
| 0 | 8 | magic `BMIRROR\x01` |
| 8 | 4 | codec: 0 = body follows as is, 1 = body follows as zstd data (a writer stores one frame) |
| 12 | 4 | reserved, zero |
| 16 | 8 | body length after decoding |

The decoded body is at most 1 GiB. It is a 64-byte header followed by five
sections in this order, with no padding:

| offset | size | field |
| --- | --- | --- |
| 0 | 4 | version, 1 |
| 4 | 4 | extent compression, 1 (zstd) |
| 8 | 4 | entry count, 1 to 2^23 |
| 12 | 4 | object count |
| 16 | 4 | extent count |
| 20 | 4 | meta length, at most 64 KiB |
| 24 | 8 | names length |
| 32 | 8 | total size: the sum of all file sizes |
| 40 | 24 | reserved, zero |

1. **meta**: opaque bytes, by convention a JSON object describing the source
   (see [Meta](#meta)). Readers do not interpret it.
2. **objects**: 40 bytes each: SHA-256 (32), object size (u64). Sorted by
   digest, unique, each between 1 byte and 64 MiB, and each referenced by at
   least one extent.
3. **entries**: 40 bytes each, described below.
4. **extents**: 16 bytes each: `object`, `offset`, `stored_len`, `raw_len`
   (u32 each).
5. **names**: the bytes of every name and symlink target.

### Entries

| offset | size | field |
| --- | --- | --- |
| 0 | 8 | size: file bytes; symlink target length; 0 for a directory |
| 8 | 8 | mtime, signed nanoseconds since the Unix epoch |
| 16 | 4 | first |
| 20 | 4 | count |
| 24 | 4 | name offset in the names section |
| 28 | 2 | name length |
| 30 | 1 | kind: 1 directory, 2 file, 3 symlink |
| 31 | 1 | reserved, zero |
| 32 | 4 | mode: permission bits only, at most 0777; 0777 for a symlink |
| 36 | 4 | reserved, zero |

`first` and `count` mean, by kind:

- **directory**: its children are entries `first .. first+count`.
- **file**: its content is extents `first .. first+count`, in order. An empty
  file has both zero.
- **symlink**: its target is `count` bytes at offset `first` of the names
  section.

There are no other kinds. Devices, sockets, FIFOs and hard links cannot be
represented; neither can ownership, setuid, setgid or sticky bits, or
extended attributes. Ownership is given by whoever presents the tree.

### Canonical layout

The same tree, with its files held in the same extents, has exactly one
encoding of the body.

- Entry 0 is the root directory. It has no name (offset and length zero).
- The entry table is in breadth-first order: the root, then the root's
  children, then the children of each directory in entry order. Each
  directory's children are the next run of entries not yet claimed, so
  `first` of the n-th directory is one past the last child of the directory
  before it, and every directory but the root comes after the directory that
  claims it.
- The children of a directory are sorted by name as bytes, and unique.
- Names are 1 to 255 bytes of valid UTF-8 without `/` or NUL, and are not `.`
  or `..`. A path (names joined by `/`) is at most 4095 bytes.
- The extent table is in entry order: each non-empty file's extents are the
  next run not yet claimed.
- The names section is in entry order too: for each entry after the root, its
  name, followed, if it is a symlink, by its target.
- A symlink target is 1 to 4095 bytes of valid UTF-8 without NUL, is relative,
  and resolved lexically from the symlink's directory never leaves the tree:
  counting components from the symlink's depth, with `..` going up one and
  `.` and empty components ignored, the count never goes below zero.
- The body is exactly the header and the five sections.

A reader must reject a manifest that breaks any rule here or in the tables
above. Then every path it serves is inside the tree, every read is inside an
object, and only directories, regular files and in-tree symlinks exist. The
reference reader is `mirror.Parse`, and
`TestParseRejectsInvalidManifests` lists the cases.

The body names the objects and the stored length of every extent, and
compressed bytes depend on the zstd implementation. So two writers given the
same tree produce the same body only if they cut it into the same extents and
none of those is compressed, or both compress them to the same bytes. The
envelope may compress the body as well, so even then the stored manifest and
its digest can differ. Vectors therefore state the extents and compare the
body, stored with codec 0.

### Meta

The reference client writes a JSON object. Readers must ignore members they
do not know. Current members:

| member | meaning |
| --- | --- |
| `client` | name and version of the writer |
| `source` | `os`, `arch` and `host` of the machine the files came from |
| `chunker` | `min`, `avg`, `max` used for this generation |
| `adapters` | which source-specific rewrites produced paths in this tree |

Paths in a manifest are relative to the home directory the tree is presented
as, not to where the files were on the source machine. A writer that takes a
file from `~/Library/Application Support/X` on macOS and wants it at
`~/.config/X` in a Linux VM writes it as `.config/X` and records the rewrite
in `adapters`.

## Committing a generation

A generation exists once the gateway has committed it. Against
`/api/v1/mirror/<workspace>/<name>`, where `<workspace>` may be `self`:

1. `POST /objects` with `{"objects": [{"digest", "size"}, ...]}`, at most
   1000 per request. The reply lists those that are missing, each with a
   presigned PUT URL. An object present at another size is missing.
2. `PUT` each missing object to its URL.
3. `POST /generations` with `{"parent": N, "manifest_digest": "..."}`, where
   `parent` is the number of the generation this one replaces, or 0 for the
   first.

The gateway checks `parent` first. It then downloads the manifest, parses it
as a reader would, and checks that every object it names which the parent
generation did not is in the store at the stated size. It then records
generation `parent + 1`. Replies:

- `201` with the generation.
- `409` with `latest`: another writer committed first. Start again from
  `latest`, which is null when the mirror has no generation.
- `422` with `missing`: objects the manifest names that are not in the store
  (they were swept, or never uploaded), at most 1000 of them. Upload them
  and commit again.
- `422` without `missing`: the manifest is not in the store, is not valid,
  or is not the bytes its digest names.

Committing the manifest of the latest generation again records a new
generation with the same contents; a writer that has nothing new does not
commit.

Objects that no kept generation names are deleted after a grace period
(48 hours by default), so an upload has that long between its first object
and its commit.

## Writer policy

Nothing here is required for a generation to be valid. It is what the
reference writer does, and what keeps uploads small when files change.

- **Small files** (up to the pack limit) are concatenated into pack objects
  of about the pack target size, each file one extent.
- **Large files** are cut at content-defined boundaries and each chunk is an
  object of its own, so an edit re-uploads only the chunks it touches and
  identical chunks are stored once.
- **Compression**: an extent is stored compressed when that saves at least
  one part in eight.

### Chunker

The chunker is Gear content-defined chunking with parameters `min`, `avg`
(a power of two) and `max`:

    table[i] for i in 0..256:
        seed = seed * 6364136223846793005 + 1442695040888963407   (mod 2^64)
        table[i] = seed
    with seed starting at 0x9E3779B97F4A7C15

    cut(data):
        if len(data) <= min: return len(data)
        hash = 0
        for i in min .. min(len(data), max):
            hash = (hash << 1) + table[data[i]]                   (mod 2^64)
            if hash & (avg - 1) == 0: return i + 1
        return min(len(data), max)

The first `min` bytes of a chunk are not hashed. It is the chunker of
`pkg/disk/layer.go` with different bounds.

## Vectors

- `chunker-vectors.json`: for deterministic inputs and three parameter sets,
  the length and SHA-256 of every chunk.
- `manifest-vector.json`: a tree with explicit extents and the stored bytes
  (codec 0) of its manifest. Entries are listed parents first and otherwise
  out of order; the encoding must not depend on the order.

`hack/mirrorstore` writes a directory as a generation with the reference
writer and checks a generation in a local directory against a directory, for
testing another implementation end to end.
