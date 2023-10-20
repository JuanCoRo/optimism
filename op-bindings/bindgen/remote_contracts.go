package main

import (
	"fmt"
	"os"
	"text/template"

	"github.com/ethereum/go-ethereum/log"
)

type remoteBindingsGenerator struct {
	contractsListFilePath                  string
	contractMetadataOutputDir              string
	bindingsPackageName                    string
	contractDataClient                     contractDataClient
	contractMetadataFileTemplate           *template.Template
	contractMetadataWithImmutablesTemplate *template.Template
	tempArtifactsDir                       string
	compareDeploymentBytecode              bool
	compareInitBytecode                    bool
	sourceChainId                          int
	compareChainId                         int
}

type remoteContract struct {
	Name                             string
	Verified                         bool
	HasImmutables                    bool
	Create2ProxyDeployed             bool
	Create2DeployerAddress           string
	DeploymentTxHashes               map[string]string
	Abi                              string
	Deployments                      map[string]string
	DeploymentSalt                   string
	DeployedBytecode                 string
	InitBytecode                     string
	UseDeploymentBytecodeFromChainId int
	UseInitBytecodeFromChainId       int
}

type contractDataClient interface {
	FetchAbi(chainId int, address string) (string, error)
	FetchDeployedBytecode(chainId int, address string) (string, error)
	FetchDeploymentTxHash(chainId int, address string) (string, error)
	FetchDeploymentData(chainId int, txHash string) (string, error)
}

// NewRemoteBindingsGenerator creates a new instance of remoteBindingsGenerator. This generator is
// used for generating Go bindings for smart contracts based on remote contract data.
//
// The generator takes several parameters:
//   - contractsListFilePath: The file path to the list of contracts for which bindings are to be generated.
//   - contractMetadataOutputDir: The directory where the generated contract metadata will be saved.
//   - bindingsPackageName: The name of the package for the generated Go bindings.
//   - contractDataClient: An instance of contractDataClient used to fetch contract data from remote sources.
//   - compareDeploymentBytecode and compareInitBytecode: Booleans indicating whether to compare deployment
//     and initialization bytecode, respectively.
//   - sourceChainId and compareChainId: Chain IDs of the source and comparison networks, respectively.
//
// The function returns a pointer to an instance of remoteBindingsGenerator, which can then be used
// to generate bindings.
//
// Example usage:
//
//	client := etherscan.NewClient(...) // create an instance of contractDataClient
//	generator := NewRemoteBindingsGenerator("contracts.txt", "output", "bindings", client, true, true, 1, 2)
func NewRemoteBindingsGenerator(
	contractsListFilePath,
	contractMetadataOutputDir,
	bindingsPackageName string,
	contractDataClient contractDataClient,
	compareDeploymentBytecode,
	compareInitBytecode bool,
	sourceChainId,
	compareChainId int,
) *remoteBindingsGenerator {
	return &remoteBindingsGenerator{
		contractsListFilePath:                  contractsListFilePath,
		contractMetadataOutputDir:              contractMetadataOutputDir,
		bindingsPackageName:                    bindingsPackageName,
		contractDataClient:                     contractDataClient,
		contractMetadataFileTemplate:           template.Must(template.New("contractMetadata").Parse(contractMetadataTemplate)),
		contractMetadataWithImmutablesTemplate: template.Must(template.New("contractMetadata").Parse(contractMetadataWithImmutablesTemplate)),
		compareDeploymentBytecode:              compareDeploymentBytecode,
		compareInitBytecode:                    compareInitBytecode,
		sourceChainId:                          sourceChainId,
		compareChainId:                         compareChainId,
	}
}

// readLocalContractList reads a JSON file specified by the given file path and
// parses it into a slice of contract names.
//
// Parameters:
// - filePath: The path to the JSON file containing the list of contract names.
//
// Returns:
// - A slice of remoteContract parsed from the JSON file.
// - An error if reading the file or parsing the JSON failed.
func (gen *remoteBindingsGenerator) readContractsList() ([]remoteContract, error) {
	var data contractsData
	err := readJSONFile(gen.contractsListFilePath, &data)
	if err != nil {
		return nil, fmt.Errorf("error reading contract list %s: %w", gen.contractsListFilePath, err)
	}
	return data.Remote, nil
}

// genBindings generates Go bindings for smart contracts based on remote contract data.
//
// The method follows these steps:
// 1. Reads the list of contracts from the contracts list file.
// 2. Creates a temporary directory for storing artifacts during the binding generation process.
// 3. Processes each contract individually to generate bindings.
func (gen *remoteBindingsGenerator) genBindings() error {
	contracts, err := gen.readContractsList()
	if err != nil {
		return err
	}

	gen.tempArtifactsDir, err = mkTempArtifactsDir()
	if err != nil {
		return err
	}
	defer func() {
		err := os.RemoveAll(gen.tempArtifactsDir)
		if err != nil {
			log.Error("Error removing temporary artifacts directory", "path", gen.tempArtifactsDir, "error", err.Error())
		} else {
			log.Debug("Successfully removed temporary artifacts directory", "path", gen.tempArtifactsDir)
		}
	}()

	err = gen.processContracts(contracts)
	if err != nil {
		return fmt.Errorf("error processing remote contracts: %w", err)
	}

	return nil
}

// contractMetadataTemplate is a Go text template for generating the metadata
// associated with a remotely sourced contracts.
//
// The template expects the following data to be provided:
// - .Package: the name of the Go package.
// - .Name: the name of the contract.
// - .DeployedBin: the binary (hex-encoded) of the deployed contract.
var contractMetadataTemplate = `// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package {{.Package}}

var {{.Name}}DeployedBin = "{{.DeployedBin}}"
func init() {
	deployedBytecodes["{{.Name}}"] = {{.Name}}DeployedBin
}
`

// contractMetadataWithImmutablesTemplate is a Go text template used to generate metadata
// for remotely sourced contracts deployed with a deployment proxy and immutable variables.
//
// The template expects the following data to be provided:
// - .Package: the name of the Go package.
// - .Name: the name of the contract.
// - .InitBin: the binary (hex-encoded) of the contract's initialization code.
// - .DeploymentSalt: the salt used during the contract's deployment.
// - .DeployerAddress: the Ethereum address of the contract's deployer.
var contractMetadataWithImmutablesTemplate = `// Code generated - DO NOT EDIT.
// This file is a generated binding and any manual changes will be lost.

package {{.Package}}

var {{.Name}}InitBin = "{{.InitBin}}"
var {{.Name}}DeploymentSalt = "{{.DeploymentSalt}}"
var {{.Name}}DeployerAddress = "{{.DeployerAddress}}"

func init() {
	initBytecodes["{{.Name}}"] = {{.Name}}InitBin
	deploymentSalts["{{.Name}}"] = {{.Name}}DeploymentSalt
	deployerAddresses["{{.Name}}"] = {{.Name}}DeployerAddress
}
`
