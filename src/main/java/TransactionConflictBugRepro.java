import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Transaction;
import com.google.common.base.Throwables;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * This program demonstrates a bug in the datastore client where it, for a lookup performed in a transaction,
 * incorrectly retries the lookup operation if it fails with an ABORTED error, without retrying the entire
 * transaction. This results in a non-retryable INVALID_ARGUMENT error, which then causes the client to give up
 * completely instead of retrying the entire transaction when using the runInTransaction method.
 *
 * Both of the below transactions are expected to succeed (t1 after being automatically retried), but due to the
 * above bug, t1 fails without being retried.
 *
 * Using google-cloud-datastore 1.73.0:
 *
 *   2019-05-10 11:43:34 - Using gcp project: styx-oss-test
 *   2019-05-10 11:43:34 - Using datastore namespace: bug-repro-5cb470ae-3d85-4a11-807c-75099cee240f
 *   2019-05-10 11:43:41 - t1: entity1 read
 *   2019-05-10 11:43:42 - t2: entity1 updated
 *   2019-05-10 11:43:42 - t2: entity2 updated
 *   2019-05-10 11:44:41 - t2: committed
 *   2019-05-10 11:44:44 - t1: failure cause: code=3, reason=INVALID_ARGUMENT
 *
 * Note: When running against firestore-in-datastore mode this repro takes longer to execute as
 *       the t2 commit seems to wait 60 seconds for t1 to complete before proceeding.
 *
 *       When running against legacy non-firestore datastore, t2 does not wait for t1 and immediately
 *       commits, causing t1 to fail.
 *
 *       Regardless, the incorrect retry behavior of the datastore client is the same in both cases.
 *
 *       It might not be possible to reproduce this issue using the datastore emulator as its behavior differs
 *       from the real datastore service on transaction conflicts.
 */
public class TransactionConflictBugRepro {

  static {
    System.setProperty(
        "java.util.logging.SimpleFormatter.format",
        "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS - %5$s%6$s%n");
  }

  private static final Logger log = Logger.getLogger(TransactionConflictBugRepro.class.getName());

  public static void main(String[] args) throws InterruptedException {
    final Datastore datastore = datastore();

    final Entity entity1 = testEntity(datastore, 1);
    final Entity entity2 = testEntity(datastore, 2);
    final Key key1 = entity1.getKey();
    final Key key2 = entity2.getKey();

    // Create two entities
    datastore.runInTransaction(ds -> {
      ds.put(entity1);
      ds.put(entity2);
      return null;
    });

    final AtomicInteger t1Attempts = new AtomicInteger();

    final CompletableFuture<Void> t1Entity1Read = new CompletableFuture<>();
    final CompletableFuture<Void> t1ReadEntity2 = new CompletableFuture<>();

    // Start a read-read transaction "t1" that will fail due to a conflict with "t2"
    final Future<Void> t1f = CompletableFuture.runAsync(() ->
        datastore.runInTransaction(t1 -> {

          // Count the number of attempts
          t1Attempts.incrementAndGet();

          // Read entity 1
          final Entity readEntity1 = t1.get(key1);
          Objects.requireNonNull(readEntity1);
          log.info("t1: entity1 read");
          t1Entity1Read.complete(null);

          // Wait for go-ahead before continuing to read entity 2
          t1ReadEntity2.get();

          // Read entity 2
          // BUG: This get will fail due to a conflict with "t2" (code 10), but the
          //      datastore client immediately retries the get in the same transaction.
          //      This causes datastore to reply with code=3 (INVALID_ARGUMENT) due
          //      to the transaction having been closed on the datastore service side.
          //      This error then causes the runInTransaction method to not retry
          //      the transaction as INVALID_ARGUMENT is not considered retriable.
          //
          //      The correct behavior here is for the datastore client to not retry the get
          //      and instead immediately roll back and retry the entire transaction.
          t1.get(key2);
          log.info("t1: entity2 read");

          return null;
        }));

    // Wait for t1 to read entity2
    t1Entity1Read.join();

    // Run a transaction "t2" conflicting with the above transaction "t1"
    // Transaction "t2" commits first and wins
    final Transaction t2 = datastore.newTransaction();
    t2.update(Entity.newBuilder(key1)
        .set("t", "t2")
        .build());
    log.info("t2: entity1 updated");
    t2.update(Entity.newBuilder(key2)
        .set("t", "t2")
        .build());
    log.info("t2: entity2 updated");
    t2.commit();
    log.info("t2: committed");

    // Let t1 continue and attempt to read entity2
    t1ReadEntity2.complete(null);

    // Verify that t1 failed with the expected bug cause
    try {
      t1f.get();
      throw new AssertionError("t1 was expected to fail due to retry bug");
    } catch (ExecutionException ex) {
      if (t1Attempts.get() != 1) {
        throw new AssertionError("t1 was expected to only have one attempt due to retry bug");
      }
      final DatastoreException cause = rootCauseOfType(ex, DatastoreException.class);
      log.info("t1: failure cause: code=" + cause.getCode() + ", reason=" + cause.getReason());
    }
  }

  private static <T extends Throwable> T rootCauseOfType(Throwable t, Class<T> cls) {
    return Throwables.getCausalChain(t).stream()
        .filter(cls::isInstance)
        .map(cls::cast)
        .reduce((x, y) -> y)
        .orElse(null);
  }

  private static Entity testEntity(Datastore datastore, int i) {
    return Entity.newBuilder(testKey(datastore, i))
        .set("i", i)
        .set("ts", Instant.now().toString())
        .set("r", UUID.randomUUID().toString())
        .build();
  }

  private static Key testKey(Datastore datastore, int i) {
    return datastore.newKeyFactory()
        .setKind("Test")
        .newKey("test-conflict-" + i);
  }

  private static Datastore datastore() {

    final DatastoreOptions options = DatastoreOptions.newBuilder()
        .setNamespace("bug-repro-" + UUID.randomUUID())
        // Note: styx-staging is a legacy non-firestore datastore project
        .setProjectId("styx-staging")
        .build();
    log.info("Using gcp project: " + options.getProjectId());
    log.info("Using datastore namespace: " + options.getNamespace());
    return options.getService();
  }
}

