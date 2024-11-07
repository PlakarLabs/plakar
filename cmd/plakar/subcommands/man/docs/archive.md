PLAKAR(ARCHIVE) - ARCHIVE (1)

# NAME

**plakar archive** - Create an archive from a Plakar snapshot

# SYNOPSIS

**plakar archive**
\[**-output**&nbsp;*pathname*]
\[**-format**&nbsp;*type*]
\[**-rebase**]
*snapshotID*

# DESCRIPTION

The
**plakar archive**
command creates an archive from the contents of a specified Plakar snapshot. Supported formats include
**tar**,
**tarball**,
and
**zip**.
Archives can be output to a specified file or to standard output, with an option to rebase (remove leading directories) from archived paths.

**-output** *pathname*

> Specify the output path for the archive file. If omitted, the archive is created with a default name based on the current date and time.

**-format** *type*

> Specify the archive format. Supported formats are:

> **tar**

> > Creates a standard .tar file.

> **tarball**

> > Creates a compressed .tar.gz file.

> **zip**

> > Creates a .zip archive.

**-rebase**

> Strip the leading path from archived files, useful for creating "flat" archives without nested directories.

# ARGUMENTS

*snapshotID*

> The ID of the snapshot to archive. A file path within the snapshot can also be specified to archive a subdirectory or single file.

# EXAMPLES

To illustrate usage:

Create a tarball of the entire snapshot:

> > plakar archive -output backup.tar.gz -format tarball abc123

Create a zip archive of a specific directory within a snapshot:

> > plakar archive -output dir.zip -format zip abc123:/path/to/dir

Archive with rebasing to remove directory structure:

> > plakar archive -rebase -format tar abc123

# DIAGNOSTICS

The **plakar archive** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as unsupported format, missing files, or permission issues.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
