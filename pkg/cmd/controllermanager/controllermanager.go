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

package controllermanager

import (
	"context"
	"fmt"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/controller"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	"github.com/spf13/cobra"
	"github.com/wzshiming/sss"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

type flagpole struct {
	Kubeconfig            string
	Master                string
	InsecureSkipTLSVerify bool
	StorageURL            []string
	Users                 utils.UsersValue
}

func NewControllerManagerCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Use:  "controller-manager",
		Long: `The OpenCIDN controller manager runs controllers that reconcile the desired state of the cluster.`,
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

			ident, err := utils.Identity()
			if err != nil {
				return fmt.Errorf("error getting identity: %v", err)
			}

			s3 := map[string]*sss.SSS{}

			for _, u := range flags.StorageURL {
				sss, err := sss.NewSSS(sss.WithURL(u))
				if err != nil {
					return fmt.Errorf("error creating s3 client: %v", err)
				}
				s3[sss.Name] = sss
			}

			ident = "controller-" + ident
			klog.Infof("Starting controller with identity: %s", ident)

			ctr, err := controller.NewControllerManager(ident, clientset, s3, flags.Users.Users)
			if err != nil {
				return fmt.Errorf("error creating controller: %v", err)
			}

			err = ctr.Start(ctx)
			if err != nil {
				return fmt.Errorf("error running controller: %v", err)
			}

			<-ctx.Done()

			return ctr.Shutdown(context.Background())
		},
	}

	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	cmd.Flags().BoolVar(&flags.InsecureSkipTLSVerify, "insecure-skip-tls-verify", false, "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure")
	cmd.Flags().StringArrayVar(&flags.StorageURL, "storage-url", flags.StorageURL, "The storage URL")
	cmd.Flags().Var(&flags.Users, "user", "List of users to domain")
	return cmd
}
