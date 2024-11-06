PLAKAR(BACKUP) - BACKUP (1)

# NAME

**plakar backup** - Create a new snapshot of a directory in a Plakar repository

# SYNOPSIS

**plakar backup**
\[**-max-concurrency**&nbsp;*number*]
\[**-tag**&nbsp;*tag*]
\[**-excludes**&nbsp;*file*]
\[**-exclude**&nbsp;*pattern*]
\[*directory*]

# DESCRIPTION

The
**plakar backup**
command creates a new snapshot of a directory in a Plakar repository, storing it with an optional tag and exclusion patterns. Snapshots can be filtered to exclude specific files or directories based on patterns provided through options.

**-max-concurrency** *number*

> Set the maximum number of parallel tasks for faster processing. Defaults to
> `8 * CPU count + 1`.

**-tag** *tag*

> Specify a tag to assign to the snapshot for easier identification.

**-excludes** *file*

> Specify a file containing exclusion patterns, one per line, to ignore files or directories in the backup.

**-exclude** *pattern*

> Specify individual exclusion patterns to ignore files or directories in the backup. This option can be repeated.

# ARGUMENTS

*directory*

> (Optional) The directory to back up. If omitted, the current working directory is used.

# EXAMPLES

To illustrate usage:

Create a snapshot of the current directory with a tag:

> > plakar backup -tag "daily\_backup"

Backup a specific directory with exclusion patterns from a file:

> > plakar backup -excludes /path/to/exclude\_file /path/to/directory

Backup a directory with specific file exclusions:

> > plakar backup -exclude "\*.tmp" -exclude "\*.log" /path/to/directory

# DIAGNOSTICS

The **plakar backup** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully, snapshot created.

&gt;0

> An error occurred, such as failure to access the repository or issues with exclusion patterns.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
