# plakar

THIS IS WORK IN PROGRESS, DO NOT USE, YOU WILL LOSE STUFF


## Quickstart

### Pushing snapshots

`plakar` works by creating a snapshot of filesystem hierachies and storing them in an efficient way.

```sh
$ plakar push /private/etc
[...]
$
```


### Listing snapshots

Available snapshots are identified by UUID identifiers and can be listed:

```sh
$ plakar ls
2a10351a-4ec5-48b1-9827-07e36c6a0ecb [2021-03-22T22:36:22Z] (size: 3.2 MB, files: 230, dirs: 38)
$
```

### Pulling snapshots

Each snapshot can be restored with a single command:

```sh
$ plakar pull 2a10351a-4ec5-48b1-9827-07e36c6a0ecb
Restored directories: 38/38 (100%)
Restored files: 230/230 (100%)
$ ls -ld private/etc/
drwxr-xr-x  78 gilles  staff  2496 22 Mar 23:40 private/etc/
$
```


## Snapshot ID

Each snapshot is assigned a UUID to allow referencing it in subcommands.

To make it easier for humans,
the `plakar` CLI provides prefix-based lookups so that the UUID does not need to be typed entirely.
Whenever a snapshot UUID is expected,
a user may provide the first characters and `plakar` will complete the missing part:

```
$ plakar ls
b1ac527d-22b1-4965-a90c-6266f77c5fcf [2021-03-22T23:17:14Z] (size: 12 MB, files: 36, dirs: 1)
$ plakar check b
b1ac527d-22b1-4965-a90c-6266f77c5fcf: OK 
$
```

In case of ambiguity,
an error will be emitted to ensure an unambiguous identifier is provided:

```
$ plakar ls
b1ac527d-22b1-4965-a90c-6266f77c5fcf [2021-03-22T23:17:14Z] (size: 12 MB, files: 36, dirs: 1)
b02c4c5d-4a32-4cb6-a29e-f81d82ddc451 [2021-03-22T23:19:04Z] (size: 12 MB, files: 36, dirs: 1)
$ plakar check b 
2021/03/23 00:21:43 plakar: snapshot ID is ambigous: b (matches 2 snapshots)
$ plakar check b1
b1ac527d-22b1-4965-a90c-6266f77c5fcf: OK           
$ 
```

## Snapshot path

Commands that refer to a resource inside a snapshot may refer to it using a snapshot-relative path.

These paths must be absolute but are prefixed with the snapshot they are relative to:

```
$ plakar ls b1:/bin/sh
5c8bea1755850ce30d0c5266eb9680b4e7a612e2a84eac8895b56523e9163218 -rwxr-xr-x     root    wheel   121 kB /bin/sh
$ plakar ls b0:/bin/sh b1:/bin/sh
5c8bea1755850ce30d0c5266eb9680b4e7a612e2a84eac8895b56523e9163218 -rwxr-xr-x     root    wheel   121 kB /bin/sh
5c8bea1755850ce30d0c5266eb9680b4e7a612e2a84eac8895b56523e9163218 -rwxr-xr-x     root    wheel   121 kB /bin/sh
$
```


## Subcommands

### cat

The `cat` subcommand reads files from snapshots sequentially,
without restoring them locally,
and writes them to the standard output:

```
$ plakar cat 2a:/private/etc/passwd|grep ^root:
root:*:0:0:System Administrator:/var/root:/bin/sh
$ plakar cat 2a:/private/etc/passwd 2a:/private/etc/group|wc -l
     266
$
```

### check

The `check` subcommand performs a health check on snapshots,
without restoring them,
by checking that all resources exist in `plakar` and matches the expected checksums:

```
$ plakar check 2a
2a10351a-4ec5-48b1-9827-07e36c6a0ecb: OK           
$
```

### diff

The `diff` subcommand performs a diff between two snapshots,
without restoring them.

If provided with two snapshot identifiers,
the command will perform an inode diff by checking if permissions, ownership or modification date has changed:
```diff
$ plakar diff 2a 2c
-  -rw-r--r--     root    wheel   3.2 kB 2020-01-01 08:00:00 +0000 UTC /private/etc/group
+  -rw-r--r--     root    wheel   3.2 kB 2021-03-22 22:58:31.955769003 +0000 UTC /private/etc/group
$
```

If provided with a file argument as third parameter,
the command will perform a file diff between the file present in both snapshots without restoring them:
```diff
$ plakar diff 2a 2c /private/etc/group
--- 2a10351a-4ec5-48b1-9827-07e36c6a0ecb:/private/etc/group
+++ 2c6c2736-ae9b-40d6-9fd9-a725a3919d9e:/private/etc/group
@@ -145,4 +145,5 @@
 com.apple.access_ssh:*:399:
 com.apple.access_remote_ae:*:400:
 _oahd:*:441:_oahd
+_foobar:*:442:_foobar
 
$
```

### ls

The `ls` subcommand is used to list snapshots or resources within snapshots,
without restoring them.

```
$ plakar ls
2a10351a-4ec5-48b1-9827-07e36c6a0ecb [2021-03-22T22:36:22Z] (size: 3.2 MB, files: 230, dirs: 38)
2c6c2736-ae9b-40d6-9fd9-a725a3919d9e [2021-03-22T22:58:35Z] (size: 3.2 MB, files: 230, dirs: 38)
$ plakar ls 2c | tail -3 
a1b83027e0b929e389bde2984078b7debf7f885051d9f9be18545aa07bebac21 -rw-r--r--     root    wheel    409 B /private/etc/asl/com.apple.mkb.internal
7bf0e7399139a3a478b9f447ceb042dee1137261b39dace27db18a93242ebdc2 -r--r--r--     root    wheel   1.8 kB /private/etc/openldap/schema/corba.ldif
5475edebf371c8c4771d6951a51a80e4c40871a01355122cac4146966d6aa58c -rw-r--r--     root    wheel   4.5 kB /private/etc/apache2/extra/httpd-mpm.conf
$
```

### pull

The `pull` subcommand is used to restore all or part of a snapshot:

```
$ plakar pull 2c
Restored directories: 38/38 (100%)
Restored files: 230/230 (100%)
$ plakar pull 2c:/private/etc/openldap
Restored directories: 2/38 (5%)
Restored files: 39/230 (16%)
$ ls -l private/etc/openldap 
total 248
-rw-r--r--   1 gilles  staff  116915 23 Mar 00:03 AppleOpenLDAP.plist
-rw-r--r--   1 gilles  staff     265 23 Mar 00:03 ldap.conf
-rw-r--r--   1 gilles  staff     265 23 Mar 00:03 ldap.conf.default
drwxr-xr-x  38 gilles  staff    1216 23 Mar 00:03 schema
$
```

### push

The `push` subcommand is used to create a snapshot of a set of directories:

```
$ plakar push /bin /sbin
[...]
push: 5c8bea1755850ce30d0c5266eb9680b4e7a612e2a84eac8895b56523e9163218 /bin/sh [0:120912]
[...]
$ plakar ls
58b02fad-bfec-42af-92c9-200cf49640d5 [2021-03-22T23:05:32Z] (size: 20 MB, files: 77, dirs: 2)
$ plakar ls 58:/bin/sh  
5c8bea1755850ce30d0c5266eb9680b4e7a612e2a84eac8895b56523e9163218 -rwxr-xr-x     root    wheel   121 kB /bin/sh
$ shasum -a 256 /bin/sh
5c8bea1755850ce30d0c5266eb9680b4e7a612e2a84eac8895b56523e9163218  /bin/sh
$
```

### rm

The `rm` subcommand is used to delete a snapshot:

```
$ plakar ls
2a10351a-4ec5-48b1-9827-07e36c6a0ecb [2021-03-22T22:36:22Z] (size: 3.2 MB, files: 230, dirs: 38)
37dbc2df-5e2c-47ba-9f59-9bc3b811f1f6 [2021-03-22T22:56:56Z] (size: 3.2 MB, files: 230, dirs: 38)
2c6c2736-ae9b-40d6-9fd9-a725a3919d9e [2021-03-22T22:58:35Z] (size: 3.2 MB, files: 230, dirs: 38)
58b02fad-bfec-42af-92c9-200cf49640d5 [2021-03-22T23:05:32Z] (size: 20 MB, files: 77, dirs: 2)
$ plakar rm 2a 37 2c 58
2a10351a-4ec5-48b1-9827-07e36c6a0ecb: OK
37dbc2df-5e2c-47ba-9f59-9bc3b811f1f6: OK
2c6c2736-ae9b-40d6-9fd9-a725a3919d9e: OK
58b02fad-bfec-42af-92c9-200cf49640d5: OK
$ plakar ls
$
```
