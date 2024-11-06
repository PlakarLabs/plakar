PLAKAR(SYNC) - SYNC (1)

# NAME

**plakar sync** - Synchronize snapshots between Plakar repositories

# SYNOPSIS

**plakar sync**
\[*snapshotID*]
*to|from|with*&nbsp;*repository*

# DESCRIPTION

The
**plakar sync**
command is used to synchronize snapshots between two Plakar repositories. Users can specify a direction ("to", "from", or "with") to control the direction of synchronization between the primary and peer repositories. If a specific snapshot ID is provided, only snapshots with matching IDs will be synchronized.

*to|from|with*

> Specifies the direction of synchronization:

> *	to - Synchronize snapshots from the local repository to the specified peer repository.

> *	from - Synchronize snapshots from the specified peer repository to the local repository.

> *	with - Synchronize snapshots in both directions, ensuring both repositories are fully synchronized.

*repository*

> Path to the peer repository to synchronize with.

# OPTIONS

No additional options are available for this command.

# ARGUMENTS

*snapshotID*

> (Optional) A partial or full snapshot ID to limit synchronization to specific snapshots that match this identifier.

# EXAMPLES

To illustrate usage:

Basic synchronization from local to peer repository:

> > plakar sync to /path/to/peer/repo

Synchronize specific snapshot to peer repository:

> > plakar sync abc123 to /path/to/peer/repo

Bi-directional synchronization with peer repository:

> > plakar sync with /path/to/peer/repo

# DIAGNOSTICS

The **plakar sync** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> General failure occurred, such as an invalid repository path, snapshot ID mismatch, or network error.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
