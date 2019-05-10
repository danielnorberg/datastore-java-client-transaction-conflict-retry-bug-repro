# Datastore Java Client Transaction Conflict Retry Bug Repro

This program demonstrates a bug in the datastore client where it, for a lookup performed in a transaction,
incorrectly incorrectly retries the lookup operation if it fails with an ABORTED error, without retrying the entire
transaction. This results in a non-retryable INVALID_ARGUMENT error, which then causes the client to give up
completely instead of retrying the entire transaction when using the runInTransaction method.

Full repro: [TransactionConflictBugRepro.java](https://github.com/danielnorberg/datastore-java-client-transaction-conflict-retry-bug-repro/blob/master/src/main/java/TransactionConflictBugRepro.java)