module github.com/Jaskaranbir/es-bank-account

go 1.15

require (
	github.com/golang/protobuf v1.4.3 // indirect
	github.com/google/go-cmp v0.5.4 // indirect
	github.com/google/uuid v1.1.3
	github.com/kr/text v0.2.0 // indirect
	github.com/nxadm/tail v1.4.6 // indirect
	github.com/onsi/ginkgo v1.14.2
	github.com/onsi/gomega v1.10.4
	github.com/pkg/errors v0.9.1
	golang.org/x/net v0.0.0-20201224014010-6772e930b67b // indirect
	golang.org/x/sync v0.0.0-20201207232520-09787c993a3a
	golang.org/x/sys v0.0.0-20201231184435-2d18734c6014 // indirect
	golang.org/x/text v0.3.4 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/validator.v2 v2.0.0-20200605151824-2b28d334fa05
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace (
	github.com/Jaskaranbir/es-bank-account/config => ./config

	github.com/Jaskaranbir/es-bank-account/domain => ./domain
	github.com/Jaskaranbir/es-bank-account/domain_test => ./domain_test
	github.com/Jaskaranbir/es-bank-account/domain/account => ./domain/account
	github.com/Jaskaranbir/es-bank-account/domain/accountview => ./domain/accountview
	github.com/Jaskaranbir/es-bank-account/domain/reader => ./domain/reader
	github.com/Jaskaranbir/es-bank-account/domain/txn => ./domain/txn
	github.com/Jaskaranbir/es-bank-account/domain/writer => ./domain/writer

	github.com/Jaskaranbir/es-bank-account/eventutil => ./eventutil
	github.com/Jaskaranbir/es-bank-account/logger => ./logger
	github.com/Jaskaranbir/es-bank-account/model => ./model
)
