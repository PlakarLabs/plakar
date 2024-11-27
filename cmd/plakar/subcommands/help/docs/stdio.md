PLAKAR-STDIO(1) - General Commands Manual

# NAME

**plakar stdio** - Start Plakar server in stdio mode

# SYNOPSIS

**plakar stdio**
\[**-no-delete**]

# DESCRIPTION

The
**plakar stdio**
command starts the Plakar server in standard input/output (stdio)
mode, allowing interaction with Plakar over stdio streams.
This command can be used for environments where communication is
expected to occur over pipes or other stdio-based mechanisms.

**-no-delete**

> Disables delete operations.
> When specified, the server will reject any requests that attempt to
> delete data.

# ARGUMENTS

None.

# EXAMPLES

Basic example to start the server in stdio mode:

	plakar stdio

Start stdio mode with delete operations disabled:

	plakar stdio -no-delete

# DIAGNOSTICS

The **plakar stdio** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred while starting the stdio server or due to an invalid
> configuration.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
