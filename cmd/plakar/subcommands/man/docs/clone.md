PLAKAR(CLONE) - CLONE (1)

# NAME

**plakar clone** - Clone a Plakar repository to a new location

# SYNOPSIS

**plakar clone**
*to*&nbsp;*repository\_path*

# DESCRIPTION

The
**plakar clone**
command creates a full clone of an existing Plakar repository, including all snapshots, packfiles, and repository states, and saves it to a specified target path. The cloned repository is assigned a new unique ID to distinguish it from the source repository.

*to* *repository\_path*

> Specifies the target path where the cloned repository should be created.

# EXAMPLES

To illustrate usage:

Clone a repository to a new location:

> > plakar clone to /path/to/new/repository

# DIAGNOSTICS

The **plakar clone** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as failure to access the source repository or to create the target repository.

# SEE ALSO

plakar(1)

macOS 15.0 - November 3, 2024
