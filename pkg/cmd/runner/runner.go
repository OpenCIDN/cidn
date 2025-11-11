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

package runner

import (
	"fmt"
	"time"

	"context"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	"github.com/OpenCIDN/cidn/pkg/runner"
	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

type flagpole struct {
	Kubeconfig            string
	Master                string
	InsecureSkipTLSVerify bool
	Duration              time.Duration
	UpdateDuration        time.Duration
	handlerName           string
}

func NewRunnerCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		UpdateDuration: 1 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "runner",
		Short: "Run the chunk runner",
		RunE: func(cmd *cobra.Command, args []string) error {
			if flags.handlerName == "" {
				ident, err := utils.Identity()
				if err != nil {
					return fmt.Errorf("error getting identity: %v", err)
				}
				flags.handlerName = ident
			}
			var config *rest.Config
			var err error
			if flags.Kubeconfig != "" || flags.Master != "" {
				config, err = clientcmd.BuildConfigFromFlags(flags.Master, flags.Kubeconfig)
			} else {
				config, err = rest.InClusterConfig()
			}
			if err != nil {
				return fmt.Errorf("error getting config: %v", err)
			}
			config.TLSClientConfig.Insecure = flags.InsecureSkipTLSVerify
			config.RateLimiter = flowcontrol.NewFakeAlwaysRateLimiter()

			clientset, err := versioned.NewForConfig(config)
			if err != nil {
				return fmt.Errorf("error creating clientset: %v", err)
			}

			ident := "runner-" + flags.handlerName
			klog.Infof("Starting runner with identity: %s for duration: %v", ident, flags.Duration)

			runner := runner.NewRunner(ident, clientset, flags.UpdateDuration)

			err = runner.Start(ctx)
			if err != nil {
				return fmt.Errorf("error running runner: %v", err)
			}

			if flags.Duration > 0 {
				durationCtx, cancel := context.WithTimeout(ctx, flags.Duration)
				defer cancel()
				<-durationCtx.Done()
			} else {
				<-ctx.Done()
			}

			return runner.Shutdown(context.Background())
		},
	}

	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	cmd.Flags().BoolVar(&flags.InsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure")
	cmd.Flags().DurationVar(&flags.Duration, "duration", 0, "Duration for which the runner should run (e.g., 5m, 1h). If 0, runs indefinitely until context cancellation")
	cmd.Flags().DurationVar(&flags.UpdateDuration, "update-duration", flags.UpdateDuration, "Duration between updating chunk statuses")
	cmd.Flags().StringVar(&flags.handlerName, "handler-name", flags.handlerName, "Name of the chunk handler to use")
	return cmd
}
