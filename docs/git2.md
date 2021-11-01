Concepts
--------

object{header,sha1}, blob-object, tree-object, commit-object, tag-object

references, `refs/heads/<branch-name>`, `refs/remotes/<name>`, `refs/tags/<name>`

merge, rebase, annotated-commits, action-signature-{name,email,timestamp}

namespace
work-tree
revision-spec
merge-message
remote, remote-fetch, in-memory-remote

ignore-rules

Types
-----

AnnotatedCommit, Signature

Diff, DiffHunk, DiffDelta, ApplyLocation, ApplyOptions

Repository, ObjectType, Oid, Tree, Commit, Index
Blame, BlameOptions, BlameHunk, BlameIter

Git as DB
---------

* Has to be content addressed database.
* Commit oriented, similar to transactions in regular Key-Value store.
* Commits are time-ordered via parent child relationshipt, a single commit can have multiple parents.
  * This also implies that we may not need sequence-numbering.
* Each file can be treated as a document and stored as a blob object-type.
* Document as value, must emit a unique key, that can be treated as file path.

```
instance-api    | new, close, purge
management-api  | len, deleted_count, footprint, is_empty, is_spin, to_name, to_seqno, to_stats, validate
read-api        | get, get_versions, iter, iter_versions range range_versions, reverse, reverse_versions
write-api       | set, set_cas, insert, insert_cas delete, delete_cas, remove, remove_cas
transaction-api | commit
```

Repository
    odb, set_odb
    blob, blob_path, find_blob


Objects
-------

* Objects are immutable, signed and compressed-(zlib).
* Commit objects are DAG, which means they can have more than one parent.
* Tag can also be GPG signed.


    blob [content-size]\0       tree [content-size]\0               commit [content-size]\0                 tag [content-size]\0
    ---------------------       ---------------------               -----------------------                 --------------------
    Simple text                 100644 blob a906cb README           tree 1a738d                             object 0576fa
                                100644 blob a874b7 Rakefile         parent a11bef                           type commit
                                040000 tree fe8971 lib              author Scott Chacon                     tag v0.1
                                                                        <schacon@gmail.com> 1205602288      tagger Scott Chacon
                                                                    committer Scott Chacon                      <schacon@gmail.com> 1205624655
                                                                        <schacon@gmail.com> 1205602288      this is my v0.1 tag
                                                                    first commit


                       +------+
            +----------| Head |------------+
            |          +------+            |
            |              |               |
            |              |               |
      +--------+      +--------+        +--------+
      | Remote |      | Branch |        |  Tag   |
      +--------+      +--------+        +--------+
            |              |               |
            |              |               |
            |         +--------+           |
            +---------| Commit |-----------+
                      +--------+
                           |
                           |
                           |
                      +--------+
                  +---|  Tree  |---+
                  |   +--------+   |
                  +--------|-------+
                           |
                      +--------+
                      |  Blobs |
                      +--------+

