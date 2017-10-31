// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/spf13/cobra"
)

var sstDir string

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "badgerpp",
	Short: "Pretty-print key-value pairs in badger database",
	RunE:  runBadgerPP,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	RootCmd.PersistentFlags().StringVar(&sstDir, "dir", "",
		"Directory where the LSM tree files are located. (required)")
}

func runBadgerPP(cmd *cobra.Command, args []string) error {
	opts := badger.DefaultOptions
	opts.Dir = sstDir
	opts.ValueDir = sstDir
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = true
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		for iter.Rewind(); iter.Valid(); iter.Next() {
			item := iter.Item()
			val, err := item.Value()
			if err != nil {
				return err
			}
			fmt.Printf("Key=%s,Value=%s,UserMeta=%d,Version=%d\n",
				item.Key(), hex.EncodeToString(val[:10]), item.UserMeta(), item.Version())
		}
		return nil
	})
}
