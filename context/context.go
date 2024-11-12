package context

import "github.com/PlakarKorp/plakar/events"

type Context struct {
	events *events.Receiver

	numCPU      int
	username    string
	homeDir     string
	hostname    string
	commandLine string
	machineID   string
	keyFromFile string
	cacheDir    string

	operatingSystem string
	architecture    string
	processID       int

	cwd string
}

func NewContext() *Context {
	return &Context{
		events: events.New(),
	}
}

func (c *Context) Close() {
	c.events.Close()
}

func (c *Context) Events() *events.Receiver {
	return c.events
}

func (c *Context) SetCWD(cwd string) {
	c.cwd = cwd
}

func (c *Context) GetCWD() string {
	return c.cwd
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

func (c *Context) SetOperatingSystem(operatingSystem string) {
	c.operatingSystem = operatingSystem
}

func (c *Context) GetOperatingSystem() string {
	return c.operatingSystem
}

func (c *Context) SetArchitecture(architecture string) {
	c.architecture = architecture
}

func (c *Context) GetArchitecture() string {
	return c.architecture
}

func (c *Context) SetProcessID(processID int) {
	c.processID = processID
}

func (c *Context) GetProcessID() int {
	return c.processID
}
