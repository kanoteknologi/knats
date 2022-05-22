module github.com/kanoteknologi/knats

go 1.16

replace git.kanosolution.net/kano/appkit => git.kanosolution.net/kano/appkit v0.0.2

replace git.kanosolution.net/koloni/crowd => git.kanosolution.net/koloni/crowd v0.0.1

replace git.kanosolution.net/kano/kaos => git.kanosolution.net/kano/kaos v0.2.0

require (
	git.kanosolution.net/kano/dbflex v1.0.15
	git.kanosolution.net/kano/kaos v0.1.1
	git.kanosolution.net/kano/kext v0.2.0
	github.com/ariefdarmawan/byter v0.0.1
	github.com/ariefdarmawan/datahub v0.2.0
	github.com/ariefdarmawan/flexmgo v0.2.2
	github.com/eaciit/toolkit v0.0.0-20210610161449-593d5fadf78e
	github.com/kanoteknologi/hd v0.0.5
	github.com/kanoteknologi/khc v0.0.0-20201106040214-bfaf5bb62d14
	github.com/nats-io/nats-server/v2 v2.5.0 // indirect
	github.com/nats-io/nats.go v1.12.1
	github.com/smartystreets/goconvey v1.7.2
	google.golang.org/protobuf v1.27.1 // indirect
)
