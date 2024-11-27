PLAKAR-EXEC(1) - General Commands Manual

# NAME

**plakar exec** - Execute a file from a Plakar snapshot

# SYNOPSIS

**plakar exec**
*snapshotID*
*filepath*
\[*command\_args&nbsp;...*]

# DESCRIPTION

The
**plakar exec**
command extracts and executes a file from a Plakar snapshot.
It can be used to temporarily run executable files directly from
snapshots.
This command supports passing arguments to the extracted executable.

*snapshotID*

> The ID of the snapshot containing the file to execute.

*filepath*

> The path within the snapshot to the file to be executed.

*command\_args*

> (Optional) Additional arguments to pass to the executable.

# EXAMPLES

Execute a script from a snapshot with no arguments:

	plakar exec abc123 /path/to/script.sh

Run an executable from a snapshot with arguments:

	plakar exec abc123 /path/to/executable --option value

# DIAGNOSTICS

The **plakar exec** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as a missing file in the snapshot or an
> execution failure.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
