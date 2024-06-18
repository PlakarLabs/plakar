/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package main

import (
	"crypto/ed25519"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/PlakarLabs/plakar/agent"
	"gopkg.in/yaml.v2"
)

var publicKey, privateKey []byte = nil, nil

func init() {
	_publicKey, _privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		log.Fatalf("Could not generate keys: %s\n", err.Error())
	}
	publicKey = _publicKey
	privateKey = _privateKey
}

type Task struct {
	Name     string `yaml:"name"`
	Source   string `yaml:"source"`
	Interval string `yaml:"interval"`
	Keep     string `yaml:"keep"`
}

type Config struct {
	Tasks []Task `yaml:"tasks"`
}

func parseConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config struct {
		Tasks []Task `yaml:"tasks"`
	}

	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &Config{
		Tasks: config.Tasks,
	}, nil
}

func cmd_agent(ctx Plakar, args []string) int {
	_ = ctx

	var opt_config string
	flags := flag.NewFlagSet("agent", flag.ExitOnError)
	flags.StringVar(&opt_config, "config", "", "plakar agent configuration")
	flags.Parse(args)

	if opt_config == "" {
		fmt.Fprintf(os.Stderr, "missing plakar agent configuration\n")
		return 1
	}

	scheduler := agent.NewScheduler()

	localMode := true
	if _, err := os.Stat(opt_config); os.IsNotExist(err) {
		localMode = false
	}

	if localMode {
		cfg, err := parseConfig(opt_config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not parse configuration: %s\n", err.Error())
			return 1
		}

		notify := make(chan agent.SchedulerEvent)

		go func() {
			for event := range notify {
				switch event := event.(type) {
				case agent.TaskCompleted:
					fmt.Printf("task completed: %s\n", event.Name)
				}
			}
		}()

		for _, task := range cfg.Tasks {
			interval, err := HumanToDuration(task.Interval)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not parse interval: %s\n", err.Error())
				return 1
			}

			keep, err := HumanToDuration(task.Keep)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not parse keep: %s\n", err.Error())
				return 1
			}

			scheduler.Schedule(agent.Task{
				Name:     task.Name,
				Source:   task.Source,
				Interval: interval,
				Keep:     keep,
			}, notify)
		}
	} else {
		cl := agent.NewClient(opt_config, publicKey, privateKey)
		go cl.Run()
	}

	<-make(chan bool)
	return 0
}
