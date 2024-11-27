PLAKAR-RM(1) - General Commands Manual

# NAME

**plakar rm** - Remove snapshots from the Plakar repository

# SYNOPSIS

**plakar rm**
\[**-older**&nbsp;*date*]
\[**-tag**&nbsp;*tag*]
*snapshotID&nbsp;...*

# DESCRIPTION

The
**plakar rm**
command is used to delete snapshots from a Plakar repository.
Snapshots can be filtered for deletion by age, using the
**-older**
option, by tag, using the
**-tag**
option, or by specifying specific snapshot IDs.

**-older** *date*

> Remove snapshots older than the specified date.
> Accepted formats include relative durations
> (e.g. 2d for 2 days, 1w for 1 week)
> or specific dates in various formats
> (e.g. 2006-01-02 15:04:05).

**-tag** *tag*

> Filter snapshots by tag, deleting only those that contain the specified tag.

# ARGUMENTS

*snapshotID*

> One or more snapshot IDs to delete.
> If no snapshot IDs are provided, either the
> **-older**
> or
> **-tag**
> option must be specified to filter snapshots for deletion.

# EXAMPLES

Remove a specific snapshot by ID:

	plakar rm abc123

Remove snapshots older than 30 days:

	plakar rm -older "30d"

Remove snapshots with a specific tag:

	plakar rm -tag "backup"

Remove snapshots older than 1 year with a specific tag:

	plakar rm -older "1y" -tag "archive"

# DIAGNOSTICS

The **plakar rm** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid date format or failure to delete a
> snapshot.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
