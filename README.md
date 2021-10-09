# plakar

THIS IS WORK IN PROGRESS, DO NOT USE IN PLACE OF YOUR REGULAR BACKUPS.

IT IS NOT ALPHA, IT IS NOT EVEN PRE-ALPHA, IT IS PRE-BIGBANG.

DO YOU PLAN TO USE IT ?
- NO: GOOD, CONTINUE NOT USING IT.
- YES: PLEASE, DO NOT USE IT.

JUST IN CASE,
HERE IS A FLOW CHART TO BETTER EXPLAIN VALID CURRENT USE-CASES:

    +-----------------------+        +----+
    | SHOULD I USE PLAKAR ? | -----> | NO |
    +-----------------------+        +----+


## TODO

- general cleanup as this is my first real Go project
- re-implement server and client properly
- currently there is no cache whatsoever, performances are not ok
- implement a nice search engine
- improve the UI


## Quickstart

### Requirement

`plakar` requires Go 1.16 or higher.


### Installing the CLI

```
$ go get github.com/poolpOrg/plakar/cmd/plakar
```


### Generating the user key-pair

First thing to do is to generate the key-pair and master key:

```
$ plakar keygen
passphrase: 
passphrase (confirm): 
keypair saved to local store
$ 
```

This results in a passphrase protected key-pair being stored in `~/.plakar/plakar.key`.
Note that if you lose this file,
or forget the passphrase,
there is no way to recover your encrypted backups to the best of my knowledge.
Make sure to save the file on separate devices.


### Initializing a plakar repository

The next thing to do is to initialize a plakar repository,

```
$ plakar init     
passphrase: 
/Users/gilles/.plakar/store: store initialized
$
```

It may be desirable to create unencrypted plakar repositories,
particularly for a default local plakar,
the `-cleartext` option may be used in this case:

```
$ plakar init -cleartext
/Users/gilles/.plakar/store: store initialized
$
```

It is possible to initialize multiple repositories,
simply by providing a path to the plakar `init` subcommand:

```
$ mkdir ~/plakars

$ plakar on ~/plakars/one init -cleartext
/Users/gilles/plakars/one: store initialized

$ plakar on ~/plakars/two init
passphrase: 
/Users/gilles/plakars/two: store initialized

$ plakar init
passphrase: 
/Users/gilles/plakars/three: store initialized
$
```

ALL of the subcommands below,
including `server`,
support working on alternate plakars.

Whenever a non-default plakar is used,
it should be noted on the command line with `on`:

```
$ plakar on ~/plakars/one push /bin
$ plakar on ~/plakars/one ls
2021-10-06T20:15:17Z f1ab2ffc-eedc-47bd-ae8c-8283e6e4cd79 10 MB (files: 36, dirs: 1)
$ 
```


### Pushing snapshots

`plakar` works by creating a snapshot of filesystem hierarchies and storing them efficiently.

```sh
$ plakar push /private/etc
$
```


### Listing snapshots

Available snapshots are identified by UUID identifiers and can be listed:

```sh
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
$
```

### Pulling snapshots

Each snapshot can be restored with a single command:

```sh
$ plakar pull b3bdb2b0-115a-4198-93a4-976edf883eb5
$ ls -ld private/etc/
drwxr-xr-x  82 gilles  staff  2624  6 Oct 21:48 private/etc/
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
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
$ plakar check b3
$ echo $?
0
$
```

In case of ambiguity,
an error will be emitted to ensure an unambiguous identifier is provided:

```
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
$ plakar check b
2021/10/06 21:50:10 plakar: snapshot ID is ambiguous: b (matches 2 snapshots)
$ plakar check b3
$ 
```

## Snapshot path

Commands that refer to a resource inside a snapshot may refer to it using a snapshot-relative path.

These paths must be absolute but are prefixed with the snapshot they are relative to:

```
$ plakar ls b3:/private/etc/passwd
e45b72f5c0c0b572db4d8d3ab7e97f368ff74e62347a824decb67a84e5224d75 -rw-r--r--     root    wheel   7.6 kB /private/etc/passwd
$ plakar ls b3:/private/etc/passwd b6:/private/etc/passwd
e45b72f5c0c0b572db4d8d3ab7e97f368ff74e62347a824decb67a84e5224d75 -rw-r--r--     root    wheel   7.6 kB /private/etc/passwd
e45b72f5c0c0b572db4d8d3ab7e97f368ff74e62347a824decb67a84e5224d75 -rw-r--r--     root    wheel   7.6 kB /private/etc/passwd
$
```


## Subcommands in alphabetical order

### cat

The `cat` subcommand reads files from snapshots sequentially,
without restoring them locally,
and writes them to the standard output:

```
$ plakar cat b3:/private/etc/passwd|grep ^root:
root:*:0:0:System Administrator:/var/root:/bin/sh
$ plakar cat b3:/private/etc/passwd b3:/private/etc/group|wc -l
     267
$
```

### check

The `check` subcommand performs a health check on snapshots,
without restoring them,
by checking that all resources exist in `plakar` and match the expected checksums.
The command exits with a successful value if the snapshot is sane:

```
$ plakar check b3 && echo ok
ok
$
```



### diff

The `diff` subcommand performs a diff between two snapshots,
without restoring them.

If provided with two snapshot identifiers,
the command will perform an inode diff by checking if permissions, ownership or modification date has changed:
```diff
$ plakar diff b3 b6
$ sudo touch /private/etc/bleh
$ plakar push /private/etc
$ plakar ls                   
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
$ plakar diff b3 45
-  drwxr-xr-x     root    wheel   2.9 kB 2021-09-26 20:45:08.587949603 +0000 UTC /private/etc/
+  drwxr-xr-x     root    wheel   2.9 kB 2021-10-06 19:54:55.675866163 +0000 UTC /private/etc/
+  -rw-r--r--     root    wheel      0 B 2021-10-06 19:54:55.67585958 +0000 UTC /private/etc/bleh
$
```

If provided with a file argument as third parameter,
the command will perform a file diff between the file present in both snapshots without restoring them:
```diff
# echo _foobar:*:442:_foobar >> /private/etc/group
$ plakar push /private/etc/group
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:00:01Z a0dee33e-568e-4946-9be3-a987f939a351 3.1 MB (files: 249, dirs: 42)
$ plakar diff b3 a0 /private/etc/group
--- b3bdb2b0-115a-4198-93a4-976edf883eb5:/private/etc/group
+++ a0dee33e-568e-4946-9be3-a987f939a351:/private/etc/group
@@ -145,4 +145,5 @@
 com.apple.access_ssh:*:399:
 com.apple.access_remote_ae:*:400:
 _oahd:*:441:_oahd
+_foobar:*:442:_foobar
 
$
```

### find

The `find` subcommand is used to list snapshots containing a specific file or directory,
without restoring them.

```
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
$ plakar find etc
b3bdb2b0-115a-4198-93a4-976edf883eb5:  drwxr-xr-x     root    wheel   2.9 kB /private/etc/
b68a8f07-da5e-4b01-bd1a-78aa8156f871:  drwxr-xr-x     root    wheel   2.9 kB /private/etc/
45509672-f314-431a-9999-5a9eaa09a98b:  drwxr-xr-x     root    wheel   2.9 kB /private/etc/
$ plakar find group
b3bdb2b0-115a-4198-93a4-976edf883eb5: 4910bfe2b7e551c4e2085b12c36941d1e1063491b7292cb0dbca7c5fe0854be5 -rw-r--r--     root    wheel   3.2 kB /private/etc/group
b68a8f07-da5e-4b01-bd1a-78aa8156f871: 4910bfe2b7e551c4e2085b12c36941d1e1063491b7292cb0dbca7c5fe0854be5 -rw-r--r--     root    wheel   3.2 kB /private/etc/group
45509672-f314-431a-9999-5a9eaa09a98b: 4910bfe2b7e551c4e2085b12c36941d1e1063491b7292cb0dbca7c5fe0854be5 -rw-r--r--     root    wheel   3.2 kB /private/etc/group
$
```


### ls

The `ls` subcommand is used to list snapshots or resources within snapshots,
without restoring them.

```
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:00:01Z a0dee33e-568e-4946-9be3-a987f939a351 3.1 MB (files: 249, dirs: 42)
$ plakar ls a0 | tail -3
0235d3c1b6cf21e7043fbc98e239ee4bc648048aafaf6be1a94a576300584ef2 -r--r--r--     root    wheel    255 B /private/etc/zprofile
fb5827cb4712b7e7932d438067ec4852c8955a9ff0f55e282473684623ebdfa1 -r--r--r--     root    wheel   3.1 kB /private/etc/zshrc
1dc9a5dec35592b043715e6b5a1796df15540ebfe97b6f25fb4960183655eec9 -rw-r--r--     root    wheel   9.3 kB /private/etc/zshrc_Apple_Terminal
$
```

### pull

The `pull` subcommand is used to restore all or part of a snapshot:

```
$ plakar pull a0
$ plakar pull a0:/private/etc/openldap
$ ls -l private/etc/openldap
total 248
-rw-r--r--   1 gilles  staff  116915  6 Oct 22:02 AppleOpenLDAP.plist
-rw-r--r--   1 gilles  staff     265  6 Oct 22:02 ldap.conf
-rw-r--r--   1 gilles  staff     265  6 Oct 22:02 ldap.conf.default
drwxr-xr-x  38 gilles  staff    1216  6 Oct 21:48 schema
$ 
```

### push

The `push` subcommand is used to create a snapshot of a set of directories:

```
$ plakar push /bin /sbin
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:00:01Z a0dee33e-568e-4946-9be3-a987f939a351 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:02:56Z d79b854f-8528-41a1-91a5-a3500321919e 18 MB (files: 77, dirs: 2)
$ plakar ls d7:/bin/sh  
e07e4a504ab6ba3ad7a4e5b905e161ef719f6a05a4bd613118eb9b74ded3092b -rwxr-xr-x     root    wheel   121 kB /bin/sh
$ shasum -a 256 /bin/sh
e07e4a504ab6ba3ad7a4e5b905e161ef719f6a05a4bd613118eb9b74ded3092b  /bin/sh
$
```

### rm

The `rm` subcommand is used to delete a snapshot:

```
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:00:01Z a0dee33e-568e-4946-9be3-a987f939a351 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:02:56Z d79b854f-8528-41a1-91a5-a3500321919e 18 MB (files: 77, dirs: 2)
$ plakar rm d7
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:00:01Z a0dee33e-568e-4946-9be3-a987f939a351 3.1 MB (files: 249, dirs: 42)
$
```

### server (in progress)

The `server` subcommand creates a plakar server on top of a plakar repository.

This will be reworked completely as it was implemented simply to validate that the backup primitives could be used with a remote plakar.

```
$ plakar server 127.0.0.1:3232
```

### tarball

The `tarball` subcommand creates a tarball from a snapshot.

The tarball is output on stdout,
it should be redirected to a filename:

```
$ plakar ls
2021-10-06T19:46:58Z b3bdb2b0-115a-4198-93a4-976edf883eb5 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:49:51Z b68a8f07-da5e-4b01-bd1a-78aa8156f871 3.1 MB (files: 248, dirs: 42)
2021-10-06T19:55:19Z 45509672-f314-431a-9999-5a9eaa09a98b 3.1 MB (files: 249, dirs: 42)
2021-10-06T20:00:01Z a0dee33e-568e-4946-9be3-a987f939a351 3.1 MB (files: 249, dirs: 42)
$ plakar tarball a0dee33e > a0dee33e_private_etc.tar.gz
$ file a0dee33e_private_etc.tar.gz 
a0dee33e_private_etc.tar.gz: gzip compressed data, original size modulo 2^32 3424256
$ 
```

### ui

The `ui` subcommand launches a local web server to browse a repository.

The tarball is output on stdout,
it should be redirected to a filename:

```
$ plakar ui
Launched UI on port 40717
```
