## Project spec

The cargo project, `kvs`, builds a command-line key-value store client called
`kvs-client`, and a key-value store server called `kvs-server`, both of which in
turn call into a library called `kvs`. The client speaks to the server over
a custom protocol.

The interface to the CLI is the same as in the [previous project]. The
difference this time is in the concurrent implementation, which will be
described as we work through it.

[previous project]: ../project-3/README.md

The library interface is nearly the same except for two things. First this time
all the `KvsEngine`, `KvStore`, etc. methods take `&self` instead of `&mut
self`, and now it implements `Clone`. This is common with concurrent
data structures. Why is that? It's not that we're not going to be writing
immutable code. It _is_ though going to be shared across threads. Why might that
preclude using `&mut self` in the method signatures? If you don't know now,
it will become obvious by the end of this project.

The second is that the library in this project contains a new _trait_,
`ThreadPool`. It contains the following methods:

- `ThreadPool::new(threads: u32) -> Result<ThreadPool>`

  Creates a new thread pool, immediately spawning the specified number of
  threads.

  Returns an error if any thread fails to spawn. All previously-spawned threads
  are terminated.

- `ThreadPool::spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static`

  Spawn a function into the threadpool.

  Spawning always succeeds, but if the function panics the threadpool continues
  to operate with the same number of threads &mdash; the thread count is not
  reduced nor is the thread pool destroyed, corrupted or invalidated.
