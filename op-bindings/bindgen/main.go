package main

import (
	"fmt"
	"log"
	"os"

	"github.com/ethereum-optimism/optimism/op-bindings/etherscan"
	gethLog "github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"
)

// Initializes and runs the BindGen CLI. The CLI supports generating
// contract bindings using Foundry artifacts and/or a remote API.
//
// It has a main command of `generate` which expected one of three subcommands to be used:
//
// `all`    - Calls `local` and `remote` binding generators.
// `local`  - Generates bindings for contracts who's Forge artifacts are available locally.
// `remote` - Generates bindings for contracts who's data is sourced from a remote `contractDataClient`.
func main() {
	app := &cli.App{
		Name:  "BindGen",
		Usage: "Generate contract bindings using Foundry artifacts and/or Etherscan API",
		Commands: []*cli.Command{
			{
				Name:  "generate",
				Usage: "Generate bindings for both local and remote contracts",
				Subcommands: []*cli.Command{
					{
						Name:   "all",
						Usage:  "Generate bindings for local contracts and from Etherscan",
						Action: generateAllBindings,
						Flags:  append(localFlags(), remoteFlags()...),
					},
					{
						Name:   "local",
						Usage:  "Generate bindings for local contracts",
						Action: generateLocalBindings,
						Flags:  localFlags(),
					},
					{
						Name:   "remote",
						Usage:  "Generate bindings for contracts from a remote source",
						Action: generateRemoteBindings,
						Flags:  remoteFlags(),
					},
				},
				Flags: generateFlags(),
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		gethLog.Crit("Error staring CLI app", "error", err.Error())
	}
}

func configureLogger(c *cli.Context) error {
	logLevel := gethLog.LvlInfo
	if c.String("log-level") != "" {
		var err error
		logLevel, err = gethLog.LvlFromString(c.String("log-level"))
		if err != nil {
			// gethLog can't be used since it errored during configuration
			log.Fatalf("Error parsing provided log-level: %s", err)
		}
	}

	h := gethLog.StreamHandler(os.Stdout, gethLog.TerminalFormat(true))
	filteredHandler := gethLog.LvlFilterHandler(logLevel, h)
	gethLog.Root().SetHandler(filteredHandler)

	return nil
}

func generateAllBindings(c *cli.Context) error {
	if err := configureLogger(c); err != nil {
		gethLog.Crit("Error configuring logger", "error", err.Error())
	}
	if err := generateLocalBindings(c); err != nil {
		gethLog.Crit("Error generating local bindings", "error", err.Error())
	}
	if err := generateRemoteBindings(c); err != nil {
		gethLog.Crit("Error generating remote bindings", "error", err.Error())
	}
	return nil
}

func generateLocalBindings(c *cli.Context) error {
	if err := configureLogger(c); err != nil {
		gethLog.Crit("Error configuring logger", "error", err.Error())
	}
	if err := genLocalBindings(c.String("local-contracts"), c.String("source-maps-list"), c.String("forge-artifacts"), c.String("go-package"), c.String("monorepo-base"), c.String("metadata-out")); err != nil {
		gethLog.Crit("Error generating local bindings", "error", err.Error())
	}
	return nil
}

func generateRemoteBindings(c *cli.Context) error {
	if err := configureLogger(c); err != nil {
		gethLog.Crit("Error configuring logger", "error", err.Error())
	}

	if c.Bool("compare-deployment-bytecode") || c.Bool("compare-init-bytecode") {
		if c.String("compare-chainid") == "" {
			gethLog.Crit("In order to compare the bytecode against another chain, compare-chainid must be provided")

		}

		if c.String("compare-apikey") == "" {
			gethLog.Crit("In order to compare the bytecode against another chain, compare-apikey must be provided")

		}
	}

	var client contractDataClient
	switch c.String("client") {
	case "etherscan":
		var err error
		client, err = etherscan.NewClient(
			c.Int("source-chainid"),
			c.Int("compare-chainid"),
			c.String("source-apikey"),
			c.String("compare-apikey"),
			c.Int("api-max-retries"),
			c.Int("api-retry-delay"),
		)
		if err != nil {
			gethLog.Crit("Error initializing new Etherscan client", "error", err.Error())

		}
	default:
		gethLog.Crit(fmt.Sprintf("Unsupported client provided: %s", c.String("client")))
	}

	bindgen := NewRemoteBindingsGenerator(
		c.String("remote-contracts"),
		c.String("metadata-out"),
		c.String("go-package"),
		client,
		c.Bool("compare-deployment-bytecode"),
		c.Bool("compare-init-bytecode"),
		c.Int("source-chainid"),
		c.Int("compare-chainid"),
	)
	if err := bindgen.genBindings(); err != nil {
		gethLog.Crit("Error generating remote bindings", "error", err.Error())
	}
	return nil
}

func generateFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:     "metadata-out",
			Usage:    "Output directory to put contract metadata files in",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "go-package",
			Usage:    "Go package name given to generated files",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "monorepo-base",
			Usage:    "Path to the base of the monorepo",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "log-level",
			Usage:    "Configured the level of logs that will be printed, defaulted to info. Options: none, debug, info, warn, error, crit",
			Required: false,
		},
	}
}

func localFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:     "local-contracts",
			Usage:    "Path to file containing list of contracts to generate bindings for that have Forge artifacts available locally",
			Required: true,
		},
		&cli.StringFlag{
			Name:  "source-maps-list",
			Usage: "Comma-separated list of contracts to generate source-maps for",
		},
		&cli.StringFlag{
			Name:     "forge-artifacts",
			Usage:    "Path to forge-artifacts directory, containing compiled contract artifacts",
			Required: true,
		},
	}
}

func remoteFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:     "client",
			Usage:    "Name of remote client to connect to. Available clients: etherscan",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "remote-contracts",
			Usage:    "Path to file containing list of contracts to generate bindings for that will have ABI and bytecode sourced from a remote source",
			Required: true,
		},
		&cli.IntFlag{
			Name:     "source-chainid",
			Usage:    "Chain ID for the source chain contract data will be pulled from",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "source-apikey",
			Usage:    "API key to access remote source for source chain queries",
			Required: true,
		},
		&cli.IntFlag{
			Name:     "compare-chainid",
			Usage:    "Chain ID for the chain contract data will be compared against",
			Required: false,
		},
		&cli.StringFlag{
			Name:     "compare-apikey",
			Usage:    "API key to access remote source for contract data comparison queries",
			Required: false,
		},
		&cli.IntFlag{
			Name:  "api-max-retries",
			Usage: "Max number of retries for getting a contract's ABI from Etherscan if rate limit is reached",
			Value: 3,
		},
		&cli.IntFlag{
			Name:  "api-retry-delay",
			Usage: "Number of seconds before trying to fetch a contract's ABI from Etherscan if rate limit is reached",
			Value: 2,
		},
		&cli.BoolFlag{
			Name:  "compare-deployment-bytecode",
			Usage: "When set to true, each contract's deployment bytecode retrieved from the source chain will be compared to the bytecode retrieved from the compare chain. If the bytecode differs between the source and compare chains, there's a possibility that the bytecode from the source chain may not work as intended on an OP chain",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "compare-init-bytecode",
			Usage: "When set to true, each contract's initialization bytecode retrieved from the source chain will be compared to the bytecode retrieved from the compare chain. If the bytecode differs between the source and compare chains, there's a possibility that the bytecode from source chain may not work as intended on an OP chain",
			Value: false,
		},
	}
}
