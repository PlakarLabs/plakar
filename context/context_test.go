package context

import (
	"testing"
)

func TestContext_SettersAndGetters(t *testing.T) {
	ctx := NewContext()

	tests := []struct {
		name     string
		setter   func()
		getter   func() interface{}
		expected interface{}
	}{
		{
			name: "SetNumCPU",
			setter: func() {
				ctx.SetNumCPU(4)
			},
			getter:   func() interface{} { return ctx.GetNumCPU() },
			expected: 4,
		},
		{
			name: "SetUsername",
			setter: func() {
				ctx.SetUsername("testuser")
			},
			getter:   func() interface{} { return ctx.GetUsername() },
			expected: "testuser",
		},
		{
			name: "SetHostname",
			setter: func() {
				ctx.SetHostname("testhost")
			},
			getter:   func() interface{} { return ctx.GetHostname() },
			expected: "testhost",
		},
		{
			name: "SetCommandLine",
			setter: func() {
				ctx.SetCommandLine("test command line")
			},
			getter:   func() interface{} { return ctx.GetCommandLine() },
			expected: "test command line",
		},
		{
			name: "SetMachineID",
			setter: func() {
				ctx.SetMachineID("machine-123")
			},
			getter:   func() interface{} { return ctx.GetMachineID() },
			expected: "machine-123",
		},
		{
			name: "SetKeyFromFile",
			setter: func() {
				ctx.SetKeyFromFile("key123")
			},
			getter:   func() interface{} { return ctx.GetKeyFromFile() },
			expected: "key123",
		},
		{
			name: "SetHomeDir",
			setter: func() {
				ctx.SetHomeDir("/home/testuser")
			},
			getter:   func() interface{} { return ctx.GetHomeDir() },
			expected: "/home/testuser",
		},
		{
			name: "SetCacheDir",
			setter: func() {
				ctx.SetCacheDir("/cache/testuser")
			},
			getter:   func() interface{} { return ctx.GetCacheDir() },
			expected: "/cache/testuser",
		},
		{
			name: "SetOperatingSystem",
			setter: func() {
				ctx.SetOperatingSystem("linux")
			},
			getter:   func() interface{} { return ctx.GetOperatingSystem() },
			expected: "linux",
		},
		{
			name: "SetArchitecture",
			setter: func() {
				ctx.SetArchitecture("amd64")
			},
			getter:   func() interface{} { return ctx.GetArchitecture() },
			expected: "amd64",
		},
		{
			name: "SetProcessID",
			setter: func() {
				ctx.SetProcessID(12345)
			},
			getter:   func() interface{} { return ctx.GetProcessID() },
			expected: 12345,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setter()
			if result := test.getter(); result != test.expected {
				t.Errorf("%s failed: expected %v, got %v", test.name, test.expected, result)
			}
		})
	}
}
