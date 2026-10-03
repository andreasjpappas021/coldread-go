module coldread.apappas.dev/go/crcobra

go 1.21

require (
	coldread.apappas.dev/go v0.2.0
	github.com/spf13/cobra v1.8.0
	github.com/spf13/pflag v1.0.5
)

require github.com/inconshreveable/mousetrap v1.1.0 // indirect

// This repo's copy; ignored when the module is used from elsewhere.
replace coldread.apappas.dev/go => ../
