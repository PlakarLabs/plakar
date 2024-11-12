PLAKAR-CREATE(1) - General Commands Manual

# NAME

**plakar create** - Create a new Plakar repository

# SYNOPSIS

**plakar create**
\[**-no-encryption**]
\[**-no-compression**]
\[**-hashing**&nbsp;*algorithm*]
\[**-compression**&nbsp;*algorithm*]
\[*repository\_path*]

# DESCRIPTION

The
**plakar create**
command creates a new Plakar repository at the specified path.
Users can configure various options for encryption, compression, and
hashing when creating the repository.

**-no-encryption**

> Disable transparent encryption for the repository.
> If specified, the repository will not use encryption.

**-no-compression**

> Disable transparent compression for the repository.
> If specified, the repository will not use compression.

**-hashing** *algorithm*

> Specify the hashing algorithm to use.
> The default is "sha256".
> Other supported algorithms may be available, depending on
> implementation.

**-compression** *algorithm*

> Specify the compression algorithm to use.
> The default is "lz4".
> Other supported algorithms may be available, depending on
> implementation.

# ARGUMENTS

*repository\_path*

> (Optional) The path where the new repository will be created.
> If omitted, the repository will be created in the user's home
> directory at ".plakar".

# EXAMPLES

Create a new repository with default settings:

	plakar create

Create a new repository with specific compression algorithm:

	plakar create -compression "gzip" /path/to/repo

Create a new repository without encryption:

	plakar create -no-encryption /path/to/repo

# DIAGNOSTICS

The **plakar create** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as invalid parameters, inability to create the
> repository, or configuration issues.

# SEE ALSO

plakar(1),
plakar-repository(1)

OpenBSD 7.6 - November 12, 2024
