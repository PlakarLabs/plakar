package context

type Context struct {
	// The name of the context
	NumCPU      int
	Username    string
	HomeDir     string
	Hostname    string
	CommandLine string
	MachineID   string
	KeyFromFile string
}

func NewContext() *Context {
	return &Context{}
}

func (c *Context) SetNumCPU(numCPU int) {
	c.NumCPU = numCPU
}

func (c *Context) GetNumCPU() int {
	return c.NumCPU
}

func (c *Context) SetUsername(username string) {
	c.Username = username
}

func (c *Context) GetUsername() string {
	return c.Username
}

func (c *Context) SetHostname(hostname string) {
	c.Hostname = hostname
}

func (c *Context) GetHostname() string {
	return c.Hostname
}

func (c *Context) SetCommandLine(commandLine string) {
	c.CommandLine = commandLine
}

func (c *Context) GetCommandLine() string {
	return c.CommandLine
}

func (c *Context) SetMachineID(machineID string) {
	c.MachineID = machineID
}

func (c *Context) GetMachineID() string {
	return c.MachineID
}

func (c *Context) SetKeyFromFile(keyFromFile string) {
	c.KeyFromFile = keyFromFile
}

func (c *Context) GetKeyFromFile() string {
	return c.KeyFromFile
}

func (c *Context) SetHomeDir(homeDir string) {
	c.HomeDir = homeDir
}

func (c *Context) GetHomeDir() string {
	return c.HomeDir
}
