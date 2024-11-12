PLAKAR-SERVER(1) - General Commands Manual

# NAME

**plakar server** - Start a Plakar server instance

# SYNOPSIS

**plakar server**
\[**-protocol**&nbsp;*protocol*]
\[**-allow-delete**]
\[*address*]

# DESCRIPTION

The
**plakar server**
command starts a Plakar server instance, allowing remote interaction
with a Plakar repository over a network.
The server can operate with different protocols (\`http\` or \`plakar\`),
and can be configured to restrict delete operations for data
protection.

**-protocol** *protocol*

> Specify the protocol for the server to use.
> Options are:

> http

> > Start an HTTP server.

> plakar

> > Start a Plakar-native server (default).

**-allow-delete**

> Enable delete operations.
> By default, delete operations are disabled to prevent accidental data
> loss.

# ARGUMENTS

*address*

> (Optional) Specify the address and port for the server to listen on.
> If omitted, the server will default to ":9876".

# EXAMPLES

Start server with default Plakar protocol:

	plakar server

Start HTTP server with delete operations enabled:

	plakar server -protocol http -allow-delete :8080

# DIAGNOSTICS

The **plakar server** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> An error occurred, such as an unsupported protocol or invalid
> configuration.

# SEE ALSO

plakar(1)

OpenBSD 7.6 - November 12, 2024
