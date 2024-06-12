// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cmd

import (
	"fmt"
	sdk "github.com/cubefs/cubefs/sdk/master"
	"github.com/spf13/cobra"
)

const (
	cmdKvUse   = "kv [COMMAND]"
	cmdKvShort = "KV operation"
)

func newKvCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   cmdKvUse,
		Short: cmdKvShort,
		Args:  cobra.MinimumNArgs(0),
	}
	cmd.AddCommand(
		newKvAddCmd(client),
		newKvUpdateCmd(client),
		newKvDelCmd(client),
		newKvGetCmd(client),
	)
	return cmd
}

const (
	cmdKvAddShort    = "Add a key/value pair"
	cmdKvUpdateShort = "Update a key/value pair"
	cmdKvDelShort    = "Del a key/value pair"
	cmdKvGetShort    = "Get key/value pairs"
)

func newKvAddCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpAdd + " [KEY] [VALUE]",
		Short: cmdKvAddShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			if err = client.KvAPI().AddKvParam(args[0], args[1]); err != nil {
				return
			}
			return
		},
	}
	return cmd
}

func newKvUpdateCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpUpdate + " [KEY] [VALUE]",
		Short: cmdKvUpdateShort,
		Args:  cobra.MinimumNArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			var err error
			defer func() {
				if err != nil {
					errout("Error: %v\n", err)
				}
			}()
			if err = client.KvAPI().UpdateKvParam(args[0], args[1]); err != nil {
				return
			}
			return
		},
	}
	return cmd
}

func newKvDelCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpDelete + " [KEY]",
		Short: cmdKvDelShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := client.KvAPI().DelKvParam(args[0]); err != nil {
				err = fmt.Errorf("Delete user failed:\n%v\n", err)
				return
			}
			stdout("Delete kv pair success.\n")
			return
		},
	}
	return cmd
}

func newKvGetCmd(client *sdk.MasterClient) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   CliOpGet + " [KEY]",
		Short: cmdKvGetShort,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			result, err := client.KvAPI().GetKvParam(args[0])
			if err != nil {
				err = fmt.Errorf("Delete user failed:\n%v\n", err)
				return
			}
			stdout("Get %v Result:\n", args[0])
			stdout("%v\n", result)
			return
		},
	}
	return cmd
}
