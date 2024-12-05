package context

import (
	"github.com/PlakarKorp/plakar/encryption/keypair"
	"github.com/PlakarKorp/plakar/events"
	"github.com/PlakarKorp/plakar/logging"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type Context struct {
	Logger *logging.Logger
	Events *events.Receiver

	NumCPU      int
	Username    string
	HomeDir     string
	Hostname    string
	CommandLine string
	MachineID   string
	KeyFromFile string
	CacheDir    string
	KeyringDir  string

	OperatingSystem string
	Architecture    string
	ProcessID       int

	PlakarClient string

	Cwd string

	MaxConcurrency int

	Identity uuid.UUID
	Keypair  *keypair.KeyPair
}

func NewContext() *Context {
	return &Context{
		Events: events.New(),
	}
}

func (c *Context) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

func FromBytes(data []byte) (*Context, error) {
	var c Context

	if err := msgpack.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	c.Events = events.New()
	return &c, nil
}

func (c *Context) Close() {
	c.Events.Close()
}

func (c *Context) SetCWD(cwd string) {
	c.Cwd = cwd
}

func (c *Context) GetCWD() string {
	return c.Cwd
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

func (c *Context) SetCacheDir(cacheDir string) {
	c.CacheDir = cacheDir
}

func (c *Context) GetCacheDir() string {
	return c.CacheDir
}

func (c *Context) SetOperatingSystem(operatingSystem string) {
	c.OperatingSystem = operatingSystem
}

func (c *Context) GetOperatingSystem() string {
	return c.OperatingSystem
}

func (c *Context) SetArchitecture(architecture string) {
	c.Architecture = architecture
}

func (c *Context) GetArchitecture() string {
	return c.Architecture
}

func (c *Context) SetProcessID(processID int) {
	c.ProcessID = processID
}

func (c *Context) GetProcessID() int {
	return c.ProcessID
}

func (c *Context) SetKeyringDir(keyringDir string) {
	c.KeyringDir = keyringDir
}

func (c *Context) GetKeyringDir() string {
	return c.KeyringDir
}

func (c *Context) SetIdentity(identity uuid.UUID) {
	c.Identity = identity
}

func (c *Context) GetIdentity() uuid.UUID {
	return c.Identity
}

func (c *Context) SetKeypair(keypair *keypair.KeyPair) {
	c.Keypair = keypair
}

func (c *Context) GetKeypair() *keypair.KeyPair {
	return c.Keypair
}

func (c *Context) SetPlakarClient(plakarClient string) {
	c.PlakarClient = plakarClient
}

func (c *Context) GetPlakarClient() string {
	return c.PlakarClient
}

func (c *Context) SetMaxConcurrency(maxConcurrency int) {
	c.MaxConcurrency = maxConcurrency
}

func (c *Context) GetMaxConcurrency() int {
	return c.MaxConcurrency
}
