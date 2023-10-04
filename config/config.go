package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

type Configuration struct {
	Global       map[string]string            `yaml:"global"`
	Repositories map[string]map[string]string `yaml:"repositories"`
}

type ConfigAPI struct {
	configFilePath string
	config         Configuration
}

func NewConfigAPI(filePath string) *ConfigAPI {
	return &ConfigAPI{
		configFilePath: filePath,
		config: Configuration{
			Global:       make(map[string]string),
			Repositories: make(map[string]map[string]string),
		},
	}
}

func (c *ConfigAPI) loadConfig() error {
	data, err := os.ReadFile(c.configFilePath)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, &c.config)
}

func (c *ConfigAPI) saveConfig() error {
	data, err := yaml.Marshal(c.config)
	if err != nil {
		return err
	}
	return os.WriteFile(c.configFilePath, data, os.ModePerm)
}

func (c *ConfigAPI) ListGlobalParameters() error {
	if err := c.loadConfig(); err != nil {
		return err
	}
	for key, value := range c.config.Global {
		fmt.Printf("%s: %s\n", key, value)
	}
	return nil
}

func (c *ConfigAPI) GetGlobalParameter(key string) (string, error) {
	if err := c.loadConfig(); err != nil {
		return "", err
	}
	value, exists := c.config.Global[key]
	if !exists {
		return "", errors.New("parameter not found")
	}
	return value, nil
}

func (c *ConfigAPI) SetGlobalParameter(key string, value string) error {
	if err := c.loadConfig(); err != nil {
		return err
	}
	c.config.Global[key] = value
	return c.saveConfig()
}

func (c *ConfigAPI) GetRepositoryParameter(repo string, key string) (string, error) {
	if err := c.loadConfig(); err != nil {
		return "", err
	}
	params, exists := c.config.Repositories[repo]
	if !exists {
		return "", errors.New("repository not found")
	}
	value, exists := params[key]
	if !exists {
		return "", errors.New("parameter not found")
	}
	return value, nil
}

func (c *ConfigAPI) SetRepositoryParameter(repo string, key string, value string) error {
	if err := c.loadConfig(); err != nil {
		return err
	}
	if _, exists := c.config.Repositories[repo]; !exists {
		c.config.Repositories[repo] = make(map[string]string)
	}
	c.config.Repositories[repo][key] = value
	return c.saveConfig()
}
