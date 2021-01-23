- [Introduction](#introduction)
  * [Use-case](#use-case)
- [Run info](#run-info)
  * [Running application](#running-application)
  * [Running tests](#running-tests)
- [Architecture](#architecture)
  * [Bus](#bus)
  * [Components](#components)
  * [Event-Sourcing implementations](#event-sourcing-implementations)
  * [Logging](#logging)
  * [Error Handling](#error-handling)
  * [Testing](#testing)

---

## Introduction

This is an example of developing a bank-account like transactional-system using domain-driven-design, event-sourcing, and CQRS.

This doesn't require any additional setups/configs and will work out of box. Below are some more details about architecture and design of this application.

### Use-case

The use-case is to develop a program that accepts or declines attempts to load/withdraw funds into customers' accounts.  
Each customer is subject to various daily and weekly limits, such as:

* Maximum number of transactions in a day or week
* Maximum amount of funds loadable into an account in a day or week

These limits are currently configured using the **[config][1]**.

## Run info

### Running application

```bash
go run main.go
```

This will read transactions from `input.txt` (from project-root), and generate an `output.txt` with results. Sample [input.txt][6] and [output.txt][7] are provided.

### Running tests

Ginkgo-CLI:
```bash
ginkgo -r \
      --p \
      --v \
      --race \
      --trace \
      --progress \
      --failOnPending \
      --randomizeSuites \
      --randomizeAllSpecs
```

Go-tests:
```bash
go test -v ./...
```

## Architecture

Even though this is a single application, the design is similar to how a micro-service architecture would be implemented (using Go-routines).

### Bus

Since the design is based on Event-Sourcing, a **[Bus][0]** is used to deliver messages (commands/events) across modules.  
This Bus is really just performing fan-in and fan-out techniques using Go-channels, and uses concept of topics (called `Actions` in context of our application) like Kafka or other message-brokers out there.

### Components

Following are the major components in the system:

* **[Reader][8]**: Simulates our input-request (which would usually be sent via a REST/GraphQL-call). For now, the requests are read from an IOReader interface line-by-line (which by default is a file), and the event `TxnRead` is published on EventBus as each line is read.

* **[Creator][9]**: Validates the data-read by `Reader` and creates a transaction-request using that data.

* **[Account][10]**: Processes the transaction-requests, which includes depositing/withdrawing funds and validating transactions (such as checking for duplicate transactions, or checking that transaction doesn't exceed daily/weekly account-limits).

* **[AccountView][11]**: Stores the results of transaction-processed by account in a report-like format.

* **[Writer][12]**: Writes the provided data to an IOWriter interface (which by default is a file).

* **[ProcessManager][13]**: Handles coordinating between above routines. For example, this creates the `CreateTxn` command for `Creator` after receiving `TxnRead` event from `Reader`.

* **[Runner][14]**: Handles lifecycly of above routines.

The application-flow is as follows:

![image](https://user-images.githubusercontent.com/12961957/104849997-bba13e00-58ba-11eb-98d5-0eb077169f7b.png)


### Event-Sourcing implementations

Various components and utilities required for Event-Sourcing (such as EventStore and message-Bus) have been implemented using in-memory storage. These are part of the **[eventutil][20]** package.

### Logging

Contextful logging has been one of the key aspects, and achieving it through concurrent flows and multiple modules can be tricky.  
So we use following logging-design:

* Every module gets its own logger instance. The instance prefixes all logs from that module by module's name.

* Each operation can add its own logging prefixes (or contexts), and any further logs from that operation will use that prefix.

* Logging-levels can be specified for all modules at once, or for each individual module using env-vars (check **[StdLogger][2]** for more details).

This provides with some extensive logs which allows tracing through application easily. [Here's][3] a sample log-file with `trace`-level logs for a single transaction flow.

### Error Handling

With extensive concurrent-flows through channels, propagating errors and controlling application-flow can be tricky.  
There are two main packages to allow efficient error-handling/propagation:

* [github.com/pkg/errors][15]: Allows wrapping errors to add contextual-information.

* [golang.org/x/sync/errgroup][16]: Provides a clean interface to propagate errors from Go-routines.

Here's a sample error log-line displaying a mocked error in [AccountView-hydration][17]:
```
2021/01/23 12:02:55 error running domain-routines: Some routines returned with errors:
[txnResultView]: transaction-result-view returned with error: error in account-view routine: listener-routine exited with error: error hydrating transaction-result view: some mock critical error
```

Notice that since this mocked-error was a critical-error, this caused the application to exit fatally.  
Controlling application-flow on critical-errors is handled by **[Runner][18]** and **[ProcessManager][19]**.

### Testing

The principles of Blackbox-testing are used. We use [Ginkgo][4] and [Gomega][5] for BDD-testing.

[0]: https://github.com/Jaskaranbir/es-bank-account/blob/main/eventutil/bus.go
[1]: https://github.com/Jaskaranbir/es-bank-account/blob/main/config/config.go
[2]: https://github.com/Jaskaranbir/es-bank-account/blob/main/logger/stdlogger.go#L37
[3]: https://github.com/Jaskaranbir/es-bank-account/blob/main/_samples/run.log
[4]: https://github.com/onsi/ginkgo
[5]: https://github.com/onsi/gomega
[6]: https://github.com/Jaskaranbir/es-bank-account/blob/main/input.txt
[7]: https://github.com/Jaskaranbir/es-bank-account/blob/main/_samples/output.txt
[8]: https://github.com/Jaskaranbir/es-bank-account/tree/main/domain/reader
[9]: https://github.com/Jaskaranbir/es-bank-account/tree/main/domain/txn
[10]: https://github.com/Jaskaranbir/es-bank-account/tree/main/domain/account
[11]: https://github.com/Jaskaranbir/es-bank-account/tree/main/domain/accountview
[12]: https://github.com/Jaskaranbir/es-bank-account/tree/main/domain/writer
[13]: https://github.com/Jaskaranbir/es-bank-account/blob/main/domain/process_mgr.go
[14]: https://github.com/Jaskaranbir/es-bank-account/blob/main/domain/runner.go
[15]: github.com/pkg/errors
[16]: golang.org/x/sync/errgroup
[17]: https://github.com/Jaskaranbir/es-bank-account/blob/main/domain/accountview/txn_result_view.go#L62
[18]: https://github.com/Jaskaranbir/es-bank-account/blob/main/domain/runner.go
[19]: https://github.com/Jaskaranbir/es-bank-account/blob/main/domain/process_mgr.go
[20]: https://github.com/Jaskaranbir/es-bank-account/tree/main/eventutil
