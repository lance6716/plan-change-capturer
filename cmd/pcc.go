package cmd

import (
	"github.com/lance6716/plan-change-capturer/pkg/pcc"
	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{
		Use:   "plan-change-capturer",
		Short: "A tool used to capture plan changes among different versions of TiDB",
		Run: func(*cobra.Command, []string) {
			pcc.Run(&pcc.Config{
				OldVersion: pcc.TiDB{
					Host:       oldVersionHost,
					Port:       oldVersionPort,
					User:       oldVersionUser,
					Password:   oldVersionPassword,
					StatusPort: oldVersionStatusPort,
				},
				NewVersion: pcc.TiDB{
					Host:     newVersionHost,
					Port:     newVersionPort,
					User:     newVersionUser,
					Password: newVersionPassword,
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
	workDir string

	oldVersionHost       string
	oldVersionPort       int
	oldVersionUser       string
	oldVersionPassword   string
	oldVersionStatusPort int

	newVersionHost     string
	newVersionPort     int
	newVersionUser     string
	newVersionPassword string
)

func init() {
	cobra.OnInitialize()

	rootCmd.PersistentFlags().StringVarP(&workDir, "work-dir", "w", "", "work directory")
	rootCmd.PersistentFlags().StringVar(&oldVersionHost, "old-host", "", "old version host")
	rootCmd.PersistentFlags().IntVar(&oldVersionPort, "old-port", 4000, "old version port")
	rootCmd.PersistentFlags().StringVar(&oldVersionUser, "old-user", "root", "old version user")
	rootCmd.PersistentFlags().StringVar(&oldVersionPassword, "old-password", "", "old version password")
	rootCmd.PersistentFlags().IntVar(&oldVersionStatusPort, "old-status-port", 10080, "old version status port")
	rootCmd.PersistentFlags().StringVar(&newVersionHost, "new-host", "", "new version host")
	rootCmd.PersistentFlags().IntVar(&newVersionPort, "new-port", 4001, "new version port")
	rootCmd.PersistentFlags().StringVar(&newVersionUser, "new-user", "root", "new version user")
	rootCmd.PersistentFlags().StringVar(&newVersionPassword, "new-password", "", "new version password")
}
