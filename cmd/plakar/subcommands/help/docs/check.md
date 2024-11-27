PLAKAR(CHECK) - CHECK (1)

# NAME

**plakar check** - Check data integrity in a Plakar repository or snapshot

# SYNOPSIS

**plakar check**
\[**-concurrency**&nbsp;*number*]
\[**-fast**]
\[**-no-verify**]
\[**-quiet**]
\[*snapshotID&nbsp;...*]

# DESCRIPTION

The
**plakar check**
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

**-no-verify**

> Disable signature verification.
> This option allows to proceed with checking snapshot integrity
> regardless of an invalid snapshot signature.

**-quiet**

> Suppress output to standard output, only logging errors and warnings.

# ARGUMENTS

*snapshotID*

> (Optional) One or more snapshot IDs to verify.
> If omitted, the command checks all snapshots in the repository.

# EXAMPLES

Perform a full integrity check on all snapshots:

	plakar check

Perform a fast check on a specific snapshot:

	plakar check -fast abc123

# DIAGNOSTICS

The **plakar check** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully with no integrity issues found.

&gt;0

> An error occurred, such as corruption detected in a snapshot or
> failure to check data integrity.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
