/*
Copyright 2025 The OpenCIDN Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apiserver

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/OpenCIDN/cidn/pkg/apiserver"
	"github.com/spf13/cobra"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/term"
	"k8s.io/klog/v2"
)

const defaultEtcdPathPrefix = "/registry/opencidn.daocloud.io"

type Options struct {
	SecureServing *genericoptions.SecureServingOptionsWithLoopback

	Etcd *genericoptions.EtcdOptions
}

func (o *Options) Flags() (fs cliflag.NamedFlagSets) {

	o.SecureServing.AddFlags(fs.FlagSet("apiserver secure serving"))

	o.Etcd.AddFlags(fs.FlagSet("Etcd"))

	return fs
}

// Validate validates ServerOptions
func (o Options) Validate(args []string) error {

	errs := o.Etcd.Validate()

	return utilerrors.NewAggregate(errs)
}

// NewServerCommand provides a CLI handler for the metrics server entrypoint
func NewServerCommand(ctx context.Context) *cobra.Command {
	opts := &Options{
		SecureServing: genericoptions.NewSecureServingOptions().WithLoopback(),
		Etcd:          genericoptions.NewEtcdOptions(storagebackend.NewDefaultConfig(defaultEtcdPathPrefix, nil)),
	}

	opts.SecureServing.BindPort = 6443

	cmd := &cobra.Command{
		Short: "Launch",
		RunE: func(c *cobra.Command, args []string) error {
			if err := opts.Validate(args); err != nil {
				return err
			}
			if err := runCommand(c.Context(), opts); err != nil {
				return err
			}
			return nil
		},
	}

	fs := cmd.Flags()
	nfs := opts.Flags()
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}
	local := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	klog.InitFlags(local)
	nfs.FlagSet("logging").AddGoFlagSet(local)

	usageFmt := "Usage:\n  %s\n"
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	cmd.SetUsageFunc(func(cmd *cobra.Command) error {
		fmt.Fprintf(cmd.OutOrStderr(), usageFmt, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStderr(), nfs, cols)
		return nil
	})
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), "%s\n\n"+usageFmt, cmd.Long, cmd.UseLine())
		cliflag.PrintSections(cmd.OutOrStdout(), nfs, cols)
	})
	return cmd
}

func runCommand(ctx context.Context, o *Options) error {
	servercfg, err := apiserver.NewConfig(o.SecureServing, o.Etcd)
	if err != nil {
		return err
	}

	server, err := servercfg.Complete().New()
	if err != nil {
		return err
	}

	return server.PrepareRun().RunWithContext(ctx)
}
