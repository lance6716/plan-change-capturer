package cmd

import (
	"github.com/lance6716/plan-change-capturer/pkg/pcc"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "plan-change-capturer",
		Short: "A tool used to capture plan changes among different versions of TiDB",
		RunE: func(*cobra.Command, []string) error {
			return pcc.Run(&pcc.Config{
				OldVersion: pcc.DB{
					Host:     oldVersionHost,
					Port:     oldVersionPort,
					User:     oldVersionUser,
					Password: oldVersionPassword,
				},
			})
		},
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

var (
	workDir            string
	oldVersionHost     string
	oldVersionPort     int
	oldVersionUser     string
	oldVersionPassword string
)

func init() {
	cobra.OnInitialize()

	rootCmd.PersistentFlags().StringVarP(&workDir, "work-dir", "w", "", "work directory")
	rootCmd.PersistentFlags().StringVar(&oldVersionHost, "old-host", "", "old version host")
	rootCmd.PersistentFlags().IntVar(&oldVersionPort, "old-port", 4000, "old version port")
	rootCmd.PersistentFlags().StringVar(&oldVersionUser, "old-user", "root", "old version user")
	rootCmd.PersistentFlags().StringVar(&oldVersionPassword, "old-password", "", "old version password")
}
