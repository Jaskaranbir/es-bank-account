#!/usr/bin/env bash

set -o errexit
set -o nounset
set -e
set -x

# =========================
# Runs Ginkgo/Gomega tests
# =========================

cd /tmp

echo "Installing Ginkgo and Gomega..."
go get github.com/onsi/ginkgo/ginkgo
go get github.com/onsi/gomega/...

cd $GOPATH/src/github.com/Jaskaranbir/es-bank-account

echo "Running go tests"
ginkgo -r \
      --p \
      --v \
      --race \
      --trace \
      --progress \
      --failOnPending \
      --randomizeSuites \
      --randomizeAllSpecs
      # TODO: Integrate with codecov
      #  --cover \
      #  -coverprofile=coverage.txt \
      #  -outputdir=$(pwd)
