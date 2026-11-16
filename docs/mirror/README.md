# Mirror

A mirror puts a copy of a laptop's files under the home of a Linux VM. The
Beam Mirror app (`beam-cloud/mirror`) uploads them; this repository stores
them, pins VMs to them and serves them to the guest, in
`pkg/abstractions/vm/mirror` and `pkg/worker/mirror_layer.go`. This document
records how the pieces fit, what was measured, what the security review
found, and the store format both sides implement.

`beam setup mirror --from DMG|URL` installs the app on a Mac and opens it
with the Beam CLI's sign-in, offered first in its onboarding;
`beam --context NAME setup mirror` offers another one. Without `--from`, it
downloads the app from `downloads.beam.cloud` if it isn't installed.

## How it fits together

1. The app scans the paths the user selected, cuts files into
   content-addressed objects, uploads the ones the store does not have, and
   commits a **generation**: a manifest naming every file and the objects
   holding it.
2. The gateway serves `/api/v1/mirror/<workspace>`. It hands out presigned
   PUT URLs for missing objects, and commits a generation only after parsing
   the manifest and finding in the bucket every object the generation adds
   over its parent.
3. A VM created with `"mirror": {"name": NAME}` in its spec, or with
   `beam vm new --mirror NAME`, is pinned to the latest generation of that
   mirror. The pin is part of the VM's spec for its whole life. A fork, and a
   VM made from a snapshot or private template of a pinned VM, keeps the pin.
   A pinned VM cannot be published as a public template (see the security
   review).
4. The worker mounts the pinned generation as a FUSE filesystem and puts it
   above the image and below the VM's own disk: `lowerdir=<mirror>:<image>`.
   A generation whose meta records the home its files came from,
   `/Users/<name>` or `/home/<name>`, appears at that path, so that absolute
   paths from the laptop work in the VM. It is owned by the desktop user, or
   by root in an image without one. The gateway checks the path when it
   commits the generation and the worker again when it mounts it. A pin
   keeps the home it was made with, since the VM's disk holds its changes at
   those paths; older pins have none. Without a home, the tree appears as
   `/root`, owned by root, and in a desktop VM also as the desktop user's
   home, owned by that user. The worker reads that account from the image
   (`/opt/beam-vm/desktop-user` and `/etc/passwd`), because its uid is
   whatever the image had free: 1001 on Ubuntu 24.04, where `ubuntu` holds
   1000. VMs on a worker pinned to the same generation share one mount when
   their images have the same homes.
5. In the VM the mirrored files are ordinary files. Root and the desktop user
   can edit, move, delete and add to them. A change is written to the VM's
   disk, as a change to a file of the image is: it survives a stop and start
   and goes into a fork. The first change to a file copies the whole file to
   that disk. No change reaches the mirror, the laptop or another VM; what
   the worker mounts never changes, and only in that sense is the layer
   read-only.
6. A purge deletes the mirror's objects. Workers notice within a minute, stop
   returning file contents from the layer and drop what the kernel cached.
   A VM restarted after a purge starts without the layer and keeps its own
   writes.

The mirror layer is its own filesystem, not a layer added to the CLIP image.
It keeps no state per inode: a node ID is a position in the manifest, so the
server's memory is the manifest's decoded size however much of the tree a
guest walks, and the IDs are the same on every mount, which is what lets a
guest overlay survive a remount.

Once a generation is mounted, the worker reads its objects into the node
cache ahead of the guest, four at a time and at most 4 GiB of them: packs
first, since they hold the small files a session opens, in the order of the
shallowest file each holds, then the chunks of large files. A read waiting
for the shared download budget goes before the prefetch. The prefetch never
holds up or fails the mount, stops at the first object it cannot fetch, and
stops when the layer is unmounted or revoked.

The pin is fixed at create because the VM's root disk records changes
relative to the generation under it. A running VM is never moved to a newer
generation.

### Logins

When the mirror is at the laptop's home path, that path is the desktop
user's home. vminit points the account at it in `/etc/passwd` on every boot.
The desktop session runs there with `HOME` and `XDG_CONFIG_HOME` set to it,
after copying in the image's launchers and defaults that the home lacks. An
interactive root login, in the browser terminal or from `beam vm ssh`,
continues as the desktop user in that home, with any agent it forwarded;
`sudo -i` returns to root. SSH accepts the VM's key for the desktop user too,
so an editor can connect as the account that owns the files. Root keeps
`/root`, so that `beam vm exec`, services and root's own files stay out of
the user's home. In a VM without a desktop user the tree is root's, at the
same path, and root's logins start in `/root` as before.

The login shell follows the mirrored files: a `.zshrc` or `.zprofile`
selects Zsh, Fish's config selects Fish when the Zsh files are absent, and
Bash is the fallback. A symlinked dotfile needs both the link and its target
in the mirror.

## What was measured

### The layer under a microVM

`TestMicroVMMirrorLayerBootAndColdBoot` and
`TestMicroVMMirrorLayerPauseResumeAndFork` in
`pkg/runtime/microvm_integration_test.go` (build tag `microvm`) boot guests
on an overlay whose lowers are the mirror mount and the CLIP image mount.
They cover reads and writes, a cold boot on the same root disk after both
FUSE layers were remounted, pause and resume, and a fork restored from a
memory image onto a new overlay. A directory renamed in the guest still
resolves its lower contents after the cold boot.

Reading 2,000 files of 4 KiB in the guest took 195 to 205 µs per file
through the mirror layer and 275 to 300 µs through the image layer, over four
runs, with the mirror's objects in memory. Read it as "the layer is not the
slow part", not as a ratio; nothing here measures a first read that has to
fetch from the bucket.

### Large generations

`TestScale` (`MIRROR_SCALE_FILES=1000000`, as root) on a shared 4-core
machine, one run each:

| | 300,000 files | 1,000,000 files |
| --- | --- | --- |
| manifest, stored | 1.7 MiB | 5.5 MiB |
| manifest, decoded and held by the worker | 21.5 MiB | 71.8 MiB |
| parse and validate | 33 ms | 117 ms |
| walk and stat of every entry through the mount | 1.5 s | 4.8 s |
| heap after the walk | 25.8 MiB | 77.1 MiB |
| revoke after a purge, 50,000 files read | 0.5 s | 1.8 s |

Timings moved by about 20% between runs. The test's tree is generated, so its
manifest compresses better than a real one: real checkouts stored 22 bytes
per file, against 6 here. A decoded manifest costs 72 bytes per entry with
the test's short names. The gateway refuses a manifest whose decoded body is
over 256 MiB, about three million files.

### Chunk and pack parameters

A prototype uploaded a synthetic home directory under a sweep of parameters:
262,046 files and 7,912 MiB, made of twelve real git checkouts,
package-manager output, downloads, a tar archive and a SQLite database, a
third of its bytes git packfiles.

- **Compression**: zstd level 3 per chunk and per packed file stores 4,251
  MiB instead of 7,765 MiB. Small files shrink 3.8 times, large files 1.5
  times.
- **Chunker**: 256 KiB / 1 MiB / 4 MiB. A 1 KiB insertion into a large file
  re-uploads 2 to 3 MiB, against 150 to 200 MiB with fixed-size chunks.
- **Pack limit**: 256 KiB, the chunker's minimum. Bytes and time do not
  depend on it.
- **Pack target**: 1 MiB of stored bytes. A worker fetches whole objects so
  that it can check them against their names, so reading one small file
  costs one pack. Nothing on the write side improved with larger packs.

These are the reference writer's defaults, not format rules. The same sweep
found costs the format does not address: every generation carries a full
manifest (5.9 MB for this dataset, against 8.0 MB of new objects after
editing 1% of the small files); edits leave dead bytes in packs, which only a
repack step that does not exist would reclaim; and after `git gc` most of a
packfile is new, as is most of a SQLite file after scattered updates.

### End to end

On a local stack with LocalStack as the object store, from a stand-in home
of 39,067 files in 1.3 GiB:

- The first push uploaded 988 objects, 488.9 MiB, in 8.5 s. A VM matched
  every entry's type, mode, size, time and symlink target, and every file's
  SHA-256.
- Hashing every file in the guest from a cold node cache took 31.9 s, and
  17.6 s the second time. Emptying the node cache fourteen times during a
  pass failed no read.
- After edits on the laptop, among them 4 KiB overwritten in a SQLite file
  and 1 KiB inserted into a tar archive, the next push uploaded 4 objects,
  1.4 MiB, in 2.5 s. A new VM was ready in 1.7 s and matched; the first VM
  still matched the original.
- 29 kinds of change to mirrored files passed as root and as the desktop
  user, among them `rename(2)` of a directory, a SQLite insert and
  `git commit` in a mirrored checkout. They survived a stop and start and
  went into a fork; another VM on the same mirror had none of them.
- A gateway restart during a push did not fail it. After a forced purge the
  bucket held none of the mirror's objects and mirrored reads in the VM
  returned `EIO` within the minute.

The store was on the same machine and the gateway spoke plain HTTP, so none
of this measures a network, S3 or TLS.

## Security review

Everything a client uploads is untrusted, including the manifest.

| Boundary | What holds |
| --- | --- |
| Manifest contents | `mirror.Parse` rejects anything but directories, regular files and relative symlinks that stay inside the tree. Every extent lies inside its object. There is no ownership, setuid, device or extended attribute to carry. |
| Memory | Decompression stops at the size the manifest declares for the body and for each extent. The gateway refuses a decoded body over 256 MiB. |
| Bucket keys and paths | The gateway generates mirror ids and parses every digest as 64 hex characters before it becomes part of a key. The worker parses the pinned id as a UUID and the digest again before either becomes a key or a path. |
| Commit | The gateway reads the manifest back from the bucket, checks it against its digest and finds every object the generation adds over its parent at the stated size. |
| Object contents | A worker checks every downloaded object against its SHA-256 before using or caching it. A cached manifest is checked against the pin before it is parsed. Cached file contents are trusted as the cache verified them when they were stored. |
| Node cache | The cache is shared by every workspace on a node and a manifest can name any digest. Mirror objects are cached under a key that includes the workspace and the mirror, so a manifest finds only what its own mirror's mounts stored. |
| Mount | `ro,nosuid,nodev` with `default_permissions`. Ownership comes from the placement, not from the client. |
| Purge | The store decides whether a generation exists: a purged one is not mounted from the cache. Mounted layers are checked every minute, stop returning contents and have their kernel cache dropped. |
| Home | A manifest's `source.home` becomes a path in the guest and in its `/etc/passwd`. The gateway records it only as `/Users/<name>` or `/home/<name>` for a conservative account name, and the worker checks it again before mounting. Callers cannot set it on a pin. |
| Export | A VM's disk holds a copy of every mirrored file it changed. A pinned VM cannot be published as a public template, which would share that disk with everyone. A VM running with its layer cannot be archived as an image, from its merged root or its writable layer. Private snapshots, templates and forks stay in the workspace and keep the pin. |

What it does not cover:

- **A guest keeps what it has read.** Pages in a guest's memory, and anything
  a guest copied to its own disk, survive a purge.
- **Objects are not encrypted by the client.** They are stored as uploaded in
  the workspace bucket.
- **A laptop can put anything in its own mirror.** The checks protect the
  platform and other tenants, not a workspace from its own machines.
- **Selected files can hold credentials.** The app replaces the tokens it
  recognises, such as GitHub, npm and API tokens, with placeholders before
  upload and keeps the values as Beam secrets, which Beam adds to the VM's
  requests to those services. SSH keys, cloud sign-ins, `.netrc`, agents'
  sign-ins and browser cookies the user selects are uploaded as they are,
  and every VM pinned to the generation can read them.
- **A store that loses a manifest looks like a purge.** A worker cannot tell
  the two apart, so the layers it has mounted stop returning contents and do
  not start again.

## Known gaps

- Every generation is a whole manifest, and directory times are part of it:
  a file appearing in the home directory, outside every selected path, is
  enough for the next sync to commit a generation that differs only in the
  manifest.
- macOS files keep their macOS paths (`~/Library/Application Support/X` is
  `~/.config/X` on Linux), Mach-O binaries and native modules built on the
  Mac do not run in the VM, and absolute symlinks or links out of the tree
  cannot be represented.
- The homes are read from the image when a VM starts. An account whose uid is
  changed inside a VM afterwards is not followed.
- A VM that starts without its layer, after a purge, keeps the desktop
  user's home at the laptop's path, where it finds only what the VM wrote.
  The worker no longer sees its pin then, so it can be archived as an image
  with the mirrored files it changed.
- The prefetch downloads up to 4 GiB of a generation whether or not the
  guest reads it, and takes the reference pack limit to tell packs from
  chunks.
- `beam vm get` still shows the pin of a mirror that was purged.
- A fork's internal snapshot appears in a mirror's references under its
  UUID.

## Store format, version 1

A **mirror** is a named collection of local files kept in the workspace
bucket. A **generation** is one committed version of it. This section
specifies what a writer puts in the bucket and what a reader may rely on.
`pkg/abstractions/vm/mirror` is the reference, and its `testdata` holds the
vectors every implementation must reproduce.

### Objects

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

### Extents

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

### Manifest

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
   (see [Meta](#meta)). Readers need not interpret it.
2. **objects**: 40 bytes each: SHA-256 (32), object size (u64). Sorted by
   digest, unique, each between 1 byte and 64 MiB, and each referenced by at
   least one extent.
3. **entries**: 40 bytes each, described below.
4. **extents**: 16 bytes each: `object`, `offset`, `stored_len`, `raw_len`
   (u32 each).
5. **names**: the bytes of every name and symlink target.

#### Entries

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

#### Canonical layout

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

#### Meta

The reference client writes a JSON object. Readers must ignore members they
do not know. Current members:

| member | meaning |
| --- | --- |
| `client` | name and version of the writer |
| `source` | `os`, `arch` and `host` of the machine the files came from; `home`, the absolute path of the home directory they came from, such as `/Users/luke`; and `user`, the login name that owns it |
| `chunker` | `min`, `avg`, `max` used for this generation |
| `adapters` | which source-specific rewrites produced paths in this tree |

Paths in a manifest are relative to the home directory the tree is presented
as, not to where the files were on the source machine. A writer that takes a
file from `~/Library/Application Support/X` on macOS and wants it at
`~/.config/X` in a Linux VM writes it as `.config/X` and records the rewrite
in `adapters`.

A reader may present the tree at `source.home`, so that absolute paths the
source machine wrote into its files resolve the same way. The gateway keeps
`source.home` with a generation only when it is `/Users/<name>` or
`/home/<name>`, where `<name>` is 1 to 32 letters, digits, `_`, `.` or `-`
and does not begin with `.` or `-`. Meta that is not JSON, or names no such
home, records none; it never fails a commit.

### Committing a generation

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
generation `parent + 1`, with the `home` its meta names (see [Meta](#meta)).
Replies:

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

### Writer policy

Nothing here is required for a generation to be valid. It is what the
reference writer does, and what keeps uploads small when files change.

- **Small files** (up to the pack limit) are concatenated into pack objects
  of about the pack target size, each file one extent.
- **Large files** are cut at content-defined boundaries and each chunk is an
  object of its own, so an edit re-uploads only the chunks it touches and
  identical chunks are stored once.
- **Compression**: an extent is stored compressed when that saves at least
  one part in eight.

#### Chunker

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

### Vectors

- `chunker-vectors.json`: for deterministic inputs and three parameter sets,
  the length and SHA-256 of every chunk.
- `manifest-vector.json`: a tree with explicit extents and the stored bytes
  (codec 0) of its manifest. Entries are listed parents first and otherwise
  out of order; the encoding must not depend on the order.

`hack/mirrorstore` writes a directory as a generation with the reference
writer and checks a generation in a local directory against a directory, for
testing another implementation end to end.
