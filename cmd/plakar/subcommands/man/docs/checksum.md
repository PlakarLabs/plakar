PLAKAR-CHECKSUM(1) - General Commands Manual

# NAME

**plakar checksum** - Calculate checksums for files in a Plakar snapshot

# SYNOPSIS

**plakar checksum**
\[**-fast**]
*snapshotID&nbsp;filepath&nbsp;...*

# DESCRIPTION

The
**plakar checksum**
command calculates and displays checksums for specified files in a
Plakar snapshot.
By default, the command computes the checksum by reading the file
contents.
The
**-fast**
option enables the use of the pre-recorded checksum, which is faster
but does not verify file integrity against current contents.

**-fast**

> Return the pre-recorded checksum for the file without re-computing it
> from the file contents.

# ARGUMENTS

*snapshotID*

> The ID of the snapshot to check.

*filepath*

> One or more paths within the snapshot for which to compute or retrieve
> checksums.

# EXAMPLES

Calculate the checksum of a file within a snapshot:

	plakar checksum abc123 /path/to/file.txt

Retrieve the pre-recorded checksum for faster output:

	plakar checksum -fast abc123 /path/to/file.txt

# DIAGNOSTICS

The **plakar checksum** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as failure to retrieve a file's checksum or
> invalid snapshot ID.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
