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
			return pcc.Run(c.Context(), &pcc.Config{
				OldVersion: pcc.TiDB{
					Host:       oldVersionHost,
					Port:       oldVersionPort,
					User:       oldVersionUser,
					Password:   oldVersionPassword,
					StatusPort: oldVersionStatusPort,
					MaxConn:    oldVersionMaxConn,
				},
				NewVersion: pcc.TiDB{
					Host:     newVersionHost,
					Port:     newVersionPort,
					User:     newVersionUser,
					Password: newVersionPassword,
					MaxConn:  newVersionMaxConn,
				},
			})
		},
		SilenceErrors: true,
		SilenceUsage:  true,
	}
)

// Execute executes the root command.
func Execute(ctx context.Context) error {
	return rootCmd.ExecuteContext(ctx)
}

var (
	workDir string

	oldVersionHost       string
	oldVersionPort       int
	oldVersionUser       string
	oldVersionPassword   string
	oldVersionStatusPort int
	oldVersionMaxConn    int

	newVersionHost     string
	newVersionPort     int
	newVersionUser     string
	newVersionPassword string
	newVersionMaxConn  int
)

func init() {
	cobra.OnInitialize()

	rootCmd.PersistentFlags().StringVarP(&workDir, "work-dir", "w", "", "work directory")
	rootCmd.PersistentFlags().StringVar(&oldVersionHost, "old-host", "", "old version host")
	rootCmd.PersistentFlags().IntVar(&oldVersionPort, "old-port", 4000, "old version port")
	rootCmd.PersistentFlags().StringVar(&oldVersionUser, "old-user", "root", "old version user")
	rootCmd.PersistentFlags().StringVar(&oldVersionPassword, "old-password", "", "old version password")
	rootCmd.PersistentFlags().IntVar(&oldVersionStatusPort, "old-status-port", 10080, "old version status port")
	rootCmd.PersistentFlags().IntVar(&oldVersionMaxConn, "old-max-conn", 4, "old version max connections")

	rootCmd.PersistentFlags().StringVar(&newVersionHost, "new-host", "", "new version host")
	rootCmd.PersistentFlags().IntVar(&newVersionPort, "new-port", 4001, "new version port")
	rootCmd.PersistentFlags().StringVar(&newVersionUser, "new-user", "root", "new version user")
	rootCmd.PersistentFlags().StringVar(&newVersionPassword, "new-password", "", "new version password")
	rootCmd.PersistentFlags().IntVar(&newVersionMaxConn, "new-max-conn", 128, "new version max connections")
}
