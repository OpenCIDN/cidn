package runner

import (
	"fmt"

	"context"

	"github.com/OpenCIDN/cidn/pkg/clientset/versioned"
	"github.com/OpenCIDN/cidn/pkg/internal/utils"
	"github.com/OpenCIDN/cidn/pkg/runner"
	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type flagpole struct {
	Kubeconfig string
	Master     string
}

func NewRunnerCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}
	cmd := &cobra.Command{
		Use:   "runner",
		Short: "Run the sync runner",
		RunE: func(cmd *cobra.Command, args []string) error {

			ident, err := utils.Identity()
			if err != nil {
				return fmt.Errorf("error getting identity: %v", err)
			}
			var config *rest.Config

			if flags.Kubeconfig != "" {
				config, err = clientcmd.BuildConfigFromFlags(flags.Master, flags.Kubeconfig)
			} else {
				config, err = rest.InClusterConfig()
			}

			if err != nil {
				return fmt.Errorf("error getting config: %v", err)
			}

			clientset, err := versioned.NewForConfig(config)
			if err != nil {
				return fmt.Errorf("error creating clientset: %v", err)
			}

			// Create runner
			runner := runner.NewRunner("runner-"+ident, clientset)

			err = runner.Start(ctx)
			if err != nil {
				return fmt.Errorf("error running runner: %v", err)
			}

			<-ctx.Done()
			return nil
		},
	}

	cmd.Flags().StringVar(&flags.Kubeconfig, "kubeconfig", flags.Kubeconfig, "Path to the kubeconfig file to use")
	cmd.Flags().StringVar(&flags.Master, "master", flags.Master, "The address of the Kubernetes API server")
	return cmd
}
