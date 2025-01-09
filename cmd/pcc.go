package cmd

import (
	"context"

	"github.com/lance6716/plan-change-capturer/pkg/pcc"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "plan-change-capturer",
		Short: "A tool used to capture plan changes among different versions of TiDB",
		RunE: func(c *cobra.Command, _ []string) error {
			return pcc.Run(c.Context(), config)
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}
)

// Execute executes the root command.
func Execute(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

var config = &pcc.Config{}

func init() {
	cobra.OnInitialize()

	rootCmd.PersistentFlags().StringVarP(&config.WorkDir, "work-dir", "w", "", "work directory")

	rootCmd.PersistentFlags().StringVar(&config.OldVersion.Host, "old-host", "", "old version host")
	rootCmd.PersistentFlags().IntVar(&config.OldVersion.Port, "old-port", 4000, "old version port")
	rootCmd.PersistentFlags().StringVar(&config.OldVersion.User, "old-user", "root", "old version user")
	rootCmd.PersistentFlags().StringVar(&config.OldVersion.Password, "old-password", "", "old version password")
	rootCmd.PersistentFlags().IntVar(&config.OldVersion.StatusPort, "old-status-port", 10080, "old version status port")
	rootCmd.PersistentFlags().IntVar(&config.OldVersion.MaxConn, "old-max-conn", 4, "old version max connections")

	rootCmd.PersistentFlags().StringVar(&config.NewVersion.Host, "new-host", "", "new version host")
	rootCmd.PersistentFlags().IntVar(&config.NewVersion.Port, "new-port", 4001, "new version port")
	rootCmd.PersistentFlags().StringVar(&config.NewVersion.User, "new-user", "root", "new version user")
	rootCmd.PersistentFlags().StringVar(&config.NewVersion.Password, "new-password", "", "new version password")
	rootCmd.PersistentFlags().IntVar(&config.NewVersion.MaxConn, "new-max-conn", 128, "new version max connections")
}
