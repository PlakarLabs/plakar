PLAKAR(CAT) - CAT (1)

# NAME

**plakar cat** - Display the contents of a file from a Plakar snapshot

# SYNOPSIS

**plakar cat**
\[**-no-decompress**]
\[**-highlight**]
*snapshotID&nbsp;filepath&nbsp;...*

# DESCRIPTION

The
**plakar cat**
command outputs the contents of files within Plakar snapshots to the standard output. It can decompress compressed files and optionally apply syntax highlighting based on the file type.

**-no-decompress**

> Display the file content as-is, without attempting to decompress it, even if it is compressed.

**-highlight**

> Apply syntax highlighting to the output based on the file type.

# ARGUMENTS

*snapshotID*

> The ID of the snapshot containing the file to display.

*filepath*

> One or more file paths within the snapshot to display.

# EXAMPLES

To illustrate usage:

Display a file's contents from a snapshot:

> > plakar cat abc123 /path/to/file.txt

Display a compressed file without decompression:

> > plakar cat -no-decompress abc123 /path/to/compressed.gz

Display a file with syntax highlighting:

> > plakar cat -highlight abc123 /path/to/script.sh

# DIAGNOSTICS

The **plakar cat** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as failure to retrieve a file or decompress content.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
