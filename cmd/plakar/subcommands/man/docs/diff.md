PLAKAR-DIFF(1) - General Commands Manual

# NAME

**plakar diff** - Show differences between two snapshots or files in Plakar

# SYNOPSIS

**plakar diff**
\[**-highlight**]
*snapshotID1*\[:*path1*]
*snapshotID2*\[:*path2*]

# DESCRIPTION

The
**plakar diff**
command compares two Plakar snapshots or specific files within
snapshots.
If only snapshot IDs are provided, it compares the root directories of
each snapshot.
If file paths are specified, the command compares the individual
files.
The diff output is shown in unified diff format, with an option to
highlight differences.

**-highlight**

> Apply syntax highlighting to the diff output for readability.

# ARGUMENTS

*snapshotID1*\[:*path1*] *snapshotID2*\[:*path2*]

> The IDs of the two snapshots to compare, optionally specifying the
> files or directories within the snapshots to compare.
> If omitted, the root directories are compared.

# EXAMPLES

Compare root directories of two snapshots:

	plakar diff abc123 def456

Compare two specific files across snapshots with highlighting:

	plakar diff -highlight abc123:path/to/file.txt def456:path/to/file.txt

# DIAGNOSTICS

The **plakar diff** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid snapshot IDs, missing files, or an
> unsupported file type.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
