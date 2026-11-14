# Mirror: design notes

A mirror puts a copy of a laptop's files under the root of a Linux VM. This
document records how the pieces fit, what was measured before building them,
what the security review changed, and the questions that are still open. The
store format has its own specification in [format.md](format.md).

## How it fits together

1. A utility on the laptop (`beam-mirror`, installed by `beam setup mirror`)
   scans the configured paths, cuts files into content-addressed objects,
   uploads the ones the store does not have, and commits a **generation**: a
   manifest naming every file and the objects holding it.
2. The gateway serves `/api/v1/mirror/<workspace>`. It hands out presigned
   PUT URLs for missing objects, and commits a generation only after parsing
   the manifest and finding in the bucket every object the generation adds
   over its parent.
3. `beam vm new --mirror [NAME]` pins the new VM to the mirror's latest
   generation. The pin is part of the VM's spec for its whole life. A fork,
   and a VM made from a snapshot or template of a pinned VM, keeps the pin;
   a published template drops it.
4. The worker mounts the pinned generation as a read-only FUSE filesystem and
   puts it between the image and everything writable:
   `lowerdir=<mirror>:<image>`. The tree appears as `/root`, owned by root,
   and in a desktop VM also as the desktop user's home, owned by that user.
   The worker reads that account from the image (`/opt/beam-vm/desktop-user`
   and `/etc/passwd`), because its uid is whatever the image had free: 1001
   on Ubuntu 24.04, where `ubuntu` holds 1000. VMs on a worker pinned to the
   same generation share one mount when their images have the same homes.
5. A purge deletes the mirror's objects. Workers notice within a minute, stop
   returning file contents from the layer and drop what the kernel cached.
   A VM restarted after a purge starts without the layer and keeps its own
   writes.

The mirror layer is its own filesystem (`pkg/mirror`), not a layer added to
the CLIP image. It keeps no state per inode: a node ID is a position in the
manifest, so the server's memory is the manifest's decoded size however much
of the tree a guest walks, and the IDs are the same on every mount, which is
what lets a guest overlay survive a remount.

The pin is fixed at create because the VM's root disk records changes
relative to the generation under it. A running VM is never moved to a newer
generation.

## What was measured

### The layer under a microVM

`pkg/runtime/microvm_mirror_integration_test.go` (build tag `microvm`) boots
guests on an overlay whose lowers are the mirror mount and the CLIP image
mount. It covers reads and writes, a cold boot on the same root disk after
both FUSE layers were remounted, pause and resume, and a fork restored from a
memory image onto a new overlay. A directory renamed in the guest still
resolves its lower contents after the cold boot.

Reading 2,000 files of 4 KiB in the guest took 195 to 205 µs per file
through the mirror layer, on the first pass and on the second, and 275 to
300 µs per file through the image layer, over four runs. The test keeps the
mirror's objects in memory, so this is the cost of the path and not of a
download, and the guest clock resolves 5 µs per file. Read it as "the layer
is not the slow part", not as a ratio. Nothing here measures a first read
that has to fetch from the bucket.

### Large generations

`TestScale` (`MIRROR_SCALE_FILES=1000000`, as root) on the same shared
4-core machine, one run each:

| | 300,000 files | 1,000,000 files |
| --- | --- | --- |
| manifest, stored | 1.7 MiB | 5.5 MiB |
| manifest, decoded and held by the worker | 21.5 MiB | 71.8 MiB |
| parse and validate | 33 ms | 117 ms |
| walk and stat of every entry through the mount | 1.5 s | 4.8 s |
| heap after the walk | 25.8 MiB | 77.1 MiB |
| revoke after a purge, 50,000 files read | 0.5 s | 1.8 s |

Timings moved by about 20% between runs. The test's tree is generated, so
its manifest compresses better than a real one: the prototype below stored
22 bytes per file for real checkouts, against 6 here. A decoded manifest
costs 72 bytes per entry with the test's short names. The gateway refuses a
manifest whose decoded body is over 256 MiB, about three million files.

### Chunk and pack parameters

A Rust prototype uploaded a synthetic home directory under a sweep of
parameters: 262,046 files and 7,912 MiB after its default excludes, made of
twelve real git checkouts, package-manager output, downloads, a tar archive
and a SQLite database. The dataset is a stand-in, not a real user's home. A
third of its bytes are git packfiles. Uploads were measured against a local
directory and MinIO on loopback; times over a WAN are a model.

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

These are the reference writer's defaults. None of them is a format rule;
the format only limits an extent to 16 MiB and an object to 64 MiB.

The same measurements found costs the format does not address:

- **A full manifest per generation.** The prototype's manifest was 22 bytes
  per file after compression, 5.9 MB for this dataset. After editing 1% of
  the small files the new objects were 8.0 MB and the new manifest 5.9 MB.
- **Edits leave dead bytes in packs.** After ten rounds of editing 1% of the
  small files, 9.4% of the original pack bytes were dead and no pack was
  wholly dead. The sweep deletes whole objects only, so reclaiming that space
  needs a repack step that does not exist.
- **Git packfiles.** After `git gc`, 66 to 100% of a packfile is new at the
  chosen chunk sizes, and 40 to 84% at the smallest sizes tested.
- **SQLite.** Scattered updates re-upload 80 to 84% of the file at every
  chunk size tested.
- **Exclude rules need context.** Of eight directories `target/` pruned,
  seven were source. Of 44 that `node_modules/` pruned, 43 are checked into
  the `nodejs/node` repository. `~/.npm`, 334 MiB of cache, was kept.

### End to end on the dev stack

On a local k3d stack (gateway from the working tree, Postgres, Redis,
localstack, a microVM worker pool):

- Two generations were pushed through the gateway API. The second uploaded 3
  of its 5 objects, and a commit on a stale parent got `409` with `latest`.
- `beam vm new --mirror laptop` reported the pinned generation, and every
  file read in the guest had the SHA-256 of its source. Modes and the symlink
  were intact, and guest writes landed in the upper layer and survived a stop
  and start.
- A VM created after the second generation saw it. The earlier VM and a fork
  of it still saw the first.
- A purge without `force` was refused with the list of references. After a
  forced purge the bucket held no object of the mirror, no file content was
  readable in the guest or on the worker's mount, and names were still
  listed. A restarted VM came up without the layer and kept its own files.

Three things limit what that run shows. The objects were written by the Go
reference writer and uploaded by a script, not by the laptop utility. The
trees held six files each, so it says nothing about size. And the host
kernel had
no tc `mirred`, `u32` or `matchall`, so the worker image carried a
development-only patch that bridges the guest's tap instead of redirecting
into it.

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

What it does not cover:

- **A guest keeps what it has read.** Pages in a guest's memory, and anything
  a guest copied to its own disk, survive a purge.
- **Objects are not encrypted by the client.** They are stored as uploaded in
  the workspace bucket.
- **A laptop can put anything in its own mirror.** The checks protect the
  platform and other tenants, not a workspace from its own machines.

Three defects were found and fixed during the review and the end-to-end run:
a small zstd frame could expand to the decoder's limit rather than the
declared size; an object another tenant had cached was readable by any
manifest that named its digest; and a file read before a purge stayed
readable from the worker's page cache afterwards.

## Open questions

### Browser and application state

A spike on Linux with Chrome 157 established what carrying a logged-in
profile takes. No Mac was available, so the macOS-format database it
converted was produced by its own converter from a Linux one.

- On Linux, cookies are in `Default/Cookies`, not `Default/Network/Cookies`;
  an adapter must probe both. Values are AES-128-CBC with a `v10` prefix when
  there is no keyring. macOS uses the same prefix with a Keychain password
  and more PBKDF2 iterations, so the prefix does not say which scheme a
  database uses.
- A converter that re-encrypts each value round-trips a database between the
  two schemes, and Chrome accepted the converted profile: it sent every
  cookie, and sent none from an unconverted one.
- Chrome commits cookies about every 30 seconds through a rollback journal.
  The SQLite backup API and `VACUUM INTO` both gave a consistent copy while
  Chrome ran. A plain file copy passed too, but only because writes are rare.
- `Local Storage` is an unencrypted LevelDB and worked when copied alone.

None of the macOS half was tested: reading "Chrome Safe Storage" from the
Keychain and the consent prompt it raises, decrypting a real macOS cookie
database, and capture while macOS Chrome runs. Saved passwords and Electron
apps were not tested on either system.

Two decisions follow from this and are not made:

- **A converted cookie database is a set of bearer credentials.** The Linux
  scheme without a keyring uses a fixed, public key, so the store and every
  VM pinned to the generation hold usable session cookies. This should be
  opt-in per profile, and probably per site.
- **Where conversion runs.** The laptop holds the Keychain key, so it has to
  happen there, before upload. The manifest's `adapters` member is where a
  generation records that it did.

Application state has the same shape with fewer secrets: an adapter per
application that knows where its state lives and what is portable.

### A macOS laptop and a Linux VM

Most state that matters is files in a POSIX tree, and a manifest is relative
to the home directory rather than to where the files were. What does not
carry over:

- **Paths.** `~/Library/Application Support/X` is `~/.config/X` on Linux.
  That is a rewrite by an adapter, recorded in the manifest.
- **Binaries.** Mach-O executables, native modules under `node_modules` and
  virtual environments built on macOS do not run in the VM. Exclude rules for
  `node_modules/`, virtual environments and build output keep the common
  cases out; the rest arrive and fail when run.
- **Absolute symlinks and links out of the tree** cannot be represented.
- **macOS-only applications** have no counterpart.

### The tray application

Launching a VM from the tray is the request `beam vm new --mirror` makes, so
it needs nothing new from the gateway. Editing sync paths from the tray needs
the utility's configuration to be the single source of truth, with the tray
and the command line both writing it. The utility is being built as a core
library with a command-line front end so that a tray front end can use the
same library. Neither tray feature is built.

### Credential scanning

The utility reads every changed byte before it uploads, which is the place to
scan: a finding can stop a file from being uploaded at all. A purge is the
remedy after the fact, with the limit noted above. Not built.

## Known gaps

- The laptop utility is not finished or released. `beam setup mirror`
  downloads it from the `beam-cloud/beam-mirror` releases, so the command
  fails until a release exists, and nothing has been pushed from a real
  laptop.
- Nothing has run on macOS.
- A first read that has to download from the bucket has not been timed.
- `beam vm get` still shows the pin of a mirror that was purged.
- A fork's internal snapshot appears in a mirror's references under its
  UUID.
- `beam vm new` waits in "Creating VM" without a message when the worker
  cannot start the guest. This is not specific to mirrors.
