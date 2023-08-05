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


## Quickstart

### Requirement

`plakar` requires Go 1.18 or higher,
it may work on older versions but hasn't been tested.


### Installing the CLI

```
go install github.com/poolpOrg/plakar/cmd/plakar@latest
```

### Creating a plakar repository

The next thing to do is to create a plakar repository,

```
$ plakar create
repository passphrase:
repository passphrase (confirm):
$
```

It may be desirable to create unencrypted plakar repositories,
particularly for the default local plakar,
the `-no-encryption` option may be used in this case:

```
$ plakar create -no-encryption
$
```

It is possible to create multiple repositories,
simply by providing a path to the plakar `create` subcommand:

```
$ plakar on /tmp/plakar.1 create -no-encryption
$

$ plakar on /tmp/plakar.2 create
repository passphrase:
repository passphrase (confirm):
$
```

ALL of the subcommands below,
support working on alternate plakars.

Whenever a non-default plakar is used,
it should be noted on the command line with `on`:

```
$ plakar on /tmp/plakar.1 push /bin
$ plakar on /tmp/plakar.1 ls
2022-03-21T22:02:17Z  22cd673e    3.1 MB /private/etc
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
2022-03-21T22:02:17Z  b3bdb2b0    3.1 MB /private/etc
$
```

### Pulling snapshots

Each snapshot can be restored with a single command:

```sh
$ plakar pull b3bdb2b0
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
2022-03-21T22:02:17Z  b3bdb2b0    3.1 MB /private/etc
$ plakar check b3
$ echo $?
0
$
```

In case of ambiguity,
an error will be emitted to ensure an unambiguous identifier is provided:

```
$ plakar ls
2022-03-21T22:02:17Z  b3bdb2b0    3.1 MB /private/etc
2022-03-21T22:02:17Z  b68a8f07    3.1 MB /private/etc
$ plakar check b
2022/03/06 21:50:10 plakar: snapshot ID is ambiguous: b (matches 2 snapshots)
$ plakar check b3
$ 
```

## Snapshot path

Commands that refer to a resource inside a snapshot may refer to it using a snapshot-relative path.

These paths must be absolute but are prefixed with the snapshot they are relative to:

```
$ plakar ls b3:/private/etc/passwd
-rw-r--r--     root    wheel   7.6 kB /private/etc/passwd
$ plakar ls b3:/private/etc/passwd b6:/private/etc/passwd
-rw-r--r--     root    wheel   7.6 kB /private/etc/passwd
-rw-r--r--     root    wheel   7.6 kB /private/etc/passwd
$
```


## For up-to-date informations

For up-to-date informations,
you can read the documentation available at https://plakar.io