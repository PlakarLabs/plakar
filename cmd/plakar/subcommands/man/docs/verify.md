PLAKAR-VERIFY(1) - General Commands Manual

# NAME

**plakar verify** - Verify data integrity in a Plakar repository or snapshot

# SYNOPSIS

**plakar verify**
\[**-concurrency**&nbsp;*number*]
\[**-fast**]
\[**-quiet**]
\[*snapshotID&nbsp;...*]

# DESCRIPTION

The
**plakar verify**
command verifies the integrity of data in a Plakar repository.
It checks snapshots for consistency and validates file checksums to
ensure no corruption has occurred.
By default, the command performs a full checksum verification.
Use the
**-fast**
option to bypass checksum calculations for a faster, less thorough
integrity check.

**-concurrency** *number*

> Set the maximum number of parallel tasks for faster processing.
> Defaults to
> `8 * CPU count + 1`.

**-fast**

> Enable a faster check that skips checksum verification.
> This option performs only structural validation without confirming
> data integrity.

**-quiet**

> Suppress output to standard output, only logging errors and warnings.

# ARGUMENTS

*snapshotID*

> (Optional) One or more snapshot IDs to verify.
> If omitted, the command checks all snapshots in the repository.

# EXAMPLES

Perform a full integrity check on all snapshots:

	plakar verify

Perform a fast check on a specific snapshot:

	plakar verify -fast abc123

# DIAGNOSTICS

The **plakar verify** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully with no integrity issues found.

&gt;0

> An error occurred, such as corruption detected in a snapshot or
> failure to verify data.

# SEE ALSO

plakar(1)

OpenBSD 7.6 - November 12, 2024
