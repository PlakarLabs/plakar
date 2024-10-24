package context

type Context struct {
	// The name of the context
	numCPU      int
	username    string
	homeDir     string
	hostname    string
	commandLine string
	machineID   string
	keyFromFile string
	cacheDir    string
}

func NewContext() *Context {
	return &Context{}
}

func (c *Context) SetNumCPU(numCPU int) {
	c.numCPU = numCPU
}

func (c *Context) GetNumCPU() int {
	return c.numCPU
}

func (c *Context) SetUsername(username string) {
	c.username = username
}

func (c *Context) GetUsername() string {
	return c.username
}

func (c *Context) SetHostname(hostname string) {
	c.hostname = hostname
}

func (c *Context) GetHostname() string {
	return c.hostname
}

func (c *Context) SetCommandLine(commandLine string) {
	c.commandLine = commandLine
}

func (c *Context) GetCommandLine() string {
	return c.commandLine
}

func (c *Context) SetMachineID(machineID string) {
	c.machineID = machineID
}

func (c *Context) GetMachineID() string {
	return c.machineID
}

func (c *Context) SetKeyFromFile(keyFromFile string) {
	c.keyFromFile = keyFromFile
}

func (c *Context) GetKeyFromFile() string {
	return c.keyFromFile
}

func (c *Context) SetHomeDir(homeDir string) {
	c.homeDir = homeDir
}

func (c *Context) GetHomeDir() string {
	return c.homeDir
}

func (c *Context) SetCacheDir(cacheDir string) {
	c.cacheDir = cacheDir
}

func (c *Context) GetCacheDir() string {
	return c.cacheDir
}
