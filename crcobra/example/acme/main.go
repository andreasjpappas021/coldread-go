// A tiny cobra CLI instrumented with Coldread, for trying it end to end.
//
//	COLDREAD_PUBLIC_KEY=cr_pub_... COLDREAD_ENDPOINT=http://localhost:3180/api/ingest \
//	  go run ./example/acme deploy preview --prod ./site
//
// COLDREAD_VERIFY=1 prints what Coldread answered. ACME_REPORT=1 prints what
// Coldread detected, as JSON on stdout.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"coldread.apappas.dev/go"
	"coldread.apappas.dev/go/crcobra"
	"github.com/spf13/cobra"
)

func main() {
	endpoint := os.Getenv("COLDREAD_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:3180/api/ingest" // a demo: never production. Your CLI leaves this out.
	}
	cr := coldread.New(coldread.Options{
		Key:         os.Getenv("COLDREAD_PUBLIC_KEY"),
		Tool:        "acme",
		Version:     "1.2.0",
		OptOut:      "ACME_NO_TELEMETRY",
		Endpoint:    endpoint,
		InferParent: os.Getenv("ACME_INFER_PARENT") == "1",
	})

	root := &cobra.Command{Use: "acme", Short: "Acme's CLI", SilenceUsage: true}
	root.PersistentFlags().BoolP("verbose", "v", false, "say more")
	deploy := &cobra.Command{Use: "deploy", Short: "Deploy the site"}
	preview := &cobra.Command{
		Use:  "preview [dir]",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if os.Getenv("ACME_REPORT") == "1" {
				out, _ := json.Marshal(map[string]any{"agent": cr.Agent(), "enabled": cr.Enabled(), "headers": cr.Headers("")})
				fmt.Println(string(out))
			}
			return nil
		},
	}
	preview.Flags().Bool("prod", false, "production")
	preview.Flags().String("token", "", "API token")
	status := &cobra.Command{Use: "status", RunE: func(*cobra.Command, []string) error { return nil }}
	fail := &cobra.Command{Use: "fail", RunE: func(*cobra.Command, []string) error { return errors.New("it failed") }}
	deploy.AddCommand(preview)
	root.AddCommand(deploy, status, fail)

	crcobra.Execute(cr, root)
}
