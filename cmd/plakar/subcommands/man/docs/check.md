PLAKAR(CHECK) - CHECK (1)

# NAME

**plakar check** - Verify data integrity in a Plakar repository or snapshot

# SYNOPSIS

**plakar check**
\[**-fast**]
\[*snapshotID&nbsp;...*]

# DESCRIPTION

The
**plakar check**
command verifies the integrity of data in a Plakar repository. It checks snapshots for consistency and validates file checksums to ensure no corruption has occurred. By default, the command performs a full checksum verification. Use the
**-fast**
option to bypass checksum calculations for a faster, less thorough integrity check.

**-fast**

> Enable a faster check that skips checksum verification. This option performs only structural validation without confirming data integrity.

# ARGUMENTS

*snapshotID*

> (Optional) One or more snapshot IDs to verify. If omitted, the command checks all snapshots in the repository.

# EXAMPLES

To illustrate usage:

Perform a full integrity check on all snapshots:

> > plakar check

Perform a fast check on a specific snapshot:

> > plakar check -fast abc123

# DIAGNOSTICS

The **plakar check** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully with no integrity issues found.

&gt;0

> An error occurred, such as corruption detected in a snapshot or failure to verify data.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
