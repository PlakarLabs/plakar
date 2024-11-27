PLAKAR-UI(1) - General Commands Manual

# NAME

**plakar ui** - Launch the Plakar user interface

# SYNOPSIS

**plakar ui**
\[**-no-spawn**]
\[**-addr**&nbsp;*address*]

# DESCRIPTION

The
**plakar ui**
command is used to launch the Plakar user interface, allowing users to
interact with repositories through a web-based UI.
By default, this command spawns the user&#8217;s web browser to open the
interface.

**-no-spawn**

> Do not automatically spawn a web browser.
> The UI will launch, but the user must manually open it by navigating
> to the specified address.

**-addr** *address*

> Specify the address and port for the UI to listen on (e.g., "localhost:8080").
> If omitted, a default address may be used.

# ARGUMENTS

None.

# EXAMPLES

Basic example:

	plakar ui

Example specifying address and disabling browser spawn:

	plakar ui -addr "localhost:9090" -no-spawn

# DIAGNOSTICS

The **plakar ui** utility exits&#160;0 on success, and&#160;&gt;0 if an error occurs.

0

> Command completed successfully.

&gt;0

> A general error occurred, such as an inability to launch the UI or
> bind to the specified address.

# SEE ALSO

plakar(1)

macOS 15.0 - November 12, 2024
