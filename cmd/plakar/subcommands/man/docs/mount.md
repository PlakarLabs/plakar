PLAKAR(MOUNT) - MOUNT (1)

# NAME

**plakar mount** - Mount a Plakar snapshot as a read-only filesystem

# SYNOPSIS

**plakar mount**
*mountpoint*

# DESCRIPTION

The
**plakar mount**
command mounts a Plakar repository snapshot as a read-only filesystem at the specified
*mountpoint*.
This allows users to access snapshot contents as if they were part of the local file system, providing easy browsing and retrieval of files without needing to explicitly restore them.

This command requires a Linux or Darwin (macOS) environment.

*mountpoint*

> Specifies the directory where the snapshot will be mounted. The directory must exist and be accessible, or an error will occur.

# OPTIONS

No additional options are available for this command.

# ARGUMENTS

*mountpoint*

> A required argument specifying the directory to which the snapshot will be mounted.

# EXAMPLES

To illustrate usage:

Mount a snapshot to the specified directory:

> > plakar mount /path/to/mountpoint

# DIAGNOSTICS

The **plakar mount** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as an invalid mountpoint or failure during the mounting process.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
