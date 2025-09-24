The problem with systems like geesefs, is it requires implementing all operations in a filesystem one by one
- mv/delete
- copy
- rename
etc...

It's a lot of work.

I'm trying to imagine a filesystem / cache system where the fuse acts as a proxy to a local directory

We can write to that local dir normally, and then after a debounce period we can flush those files to object storage


some design principles
- we dont need to implement everything, we can rely on local filesystem operations and synchronization primitives to cheat
- basically, what im proposing is when you interact with fuse, you're really interacting with a local directory (the "mirrored" dir)
- however, metadata is periodically synced from the cloud which creates "virtual files" in addition to what we have locally
- as soon as once of those files is modified, it switches from a "virtual file" to a "local file", and after a debounce, its synced up to the cloud


so we have the mirror fs, but there is no cache layer, and metadata operations are slow

if the object store is the source of truth, we should be able to store a file with metadata updates on sync
like AOF?


requirements
- small file reads are fast
- content addressed (natively) - hash in metadata somewhere
- metadata stored cold on disk (object storage), and second tier cache in hot nosql storage (i.e. redis)


what im thinking is that when a directory changes, we update all children

