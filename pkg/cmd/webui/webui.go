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

package webui

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/webui"
	"github.com/gorilla/handlers"
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
	Port                  int
	UpdateInterval        time.Duration
}

func NewWebUICommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Use:   "webui",
		Short: "Run the web UI server",
		RunE: func(cmd *cobra.Command, args []string) error {
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

			handler := webui.NewHandler(clientset, flags.UpdateInterval)
			server := &http.Server{
				Addr:    fmt.Sprintf(":%d", flags.Port),
				Handler: handlers.CompressHandler(handler),
			}

			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					klog.Errorf("Error starting web UI server: %v", err)
				}
			}()

			<-ctx.Done()
			if err := server.Shutdown(context.Background()); err != nil {
				klog.Errorf("Error shutting down web UI server: %v", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	cmd.Flags().BoolVar(&flags.InsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure")
	cmd.Flags().IntVar(&flags.Port, "port", 8080, "The port to serve the web UI on")
	cmd.Flags().DurationVar(&flags.UpdateInterval, "update-interval", 1*time.Second, "The rate at which the WebUI pushes events to the frontend")
	return cmd
}
