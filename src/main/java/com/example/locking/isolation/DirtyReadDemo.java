package com.example.locking.isolation;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * ANOMALY: DIRTY READ
 * ===================
 * Isolation level where this occurs: READ UNCOMMITTED
 * Prevented by:                      READ COMMITTED and above
 *
 * What is a Dirty Read?
 *   Transaction-2 reads data written by Transaction-1 that has NOT yet been
 *   committed. If Transaction-1 then rolls back, Transaction-2 has acted on
 *   data that technically never existed in the database.
 *
 * Real-world consequence:
 *   - A payment system reads an account balance of $0 (written by a pending
 *     transfer TX). It blocks the payment. The transfer TX rolls back — the
 *     balance is actually $1000. The payment was incorrectly blocked.
 *
 * This demo shows:
 *   A) The PROBLEM  — READ_UNCOMMITTED reader sees rolled-back data.
 *   B) The FIX      — READ_COMMITTED reader never sees uncommitted data.
 *
 * SQL to reproduce:
 *   -- Session 1 (TX-1):
 *   BEGIN;
 *   UPDATE accounts SET balance = 0 WHERE id = 'acc-1';
 *   -- (do NOT commit yet)
 *
 *   -- Session 2 (TX-2, READ UNCOMMITTED):
 *   SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
 *   SELECT balance FROM accounts WHERE id = 'acc-1';
 *   -- Returns 0  <- DIRTY READ
 *
 *   -- Session 1:
 *   ROLLBACK;
 *   -- balance is actually still 1000; TX-2 acted on ghost data
 */
public class DirtyReadDemo {

    private final InMemoryDatabase db;

    public DirtyReadDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Part A: PROBLEM — READ_UNCOMMITTED sees rolled-back data
    // -------------------------------------------------------------------------

    public void demonstrateProblem() throws InterruptedException {
        System.out.println("\n--- DIRTY READ: PROBLEM (READ_UNCOMMITTED) ---");
        db.reset();

        CountDownLatch tx1HasWritten  = new CountDownLatch(1);
        CountDownLatch tx2HasRead     = new CountDownLatch(1);

        // TX-1: writes an uncommitted change, then rolls back
        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-Writer");
            System.out.println("[TX1] BEGIN: setting acc-1 balance to 0 (NOT committed yet)");
            Account dirty = db.findAccount("acc-1").orElseThrow();
            dirty.setBalance(0);
            db.writeUncommitted(dirty);       // goes to dirty-write buffer only
            tx1HasWritten.countDown();

            // Wait for TX2 to read the dirty value before rolling back
            try { tx2HasRead.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            System.out.println("[TX1] ROLLBACK — balance of acc-1 was never really 0!");
            db.rollbackUncommitted("acc-1");  // discard the uncommitted write
        }, "TX1-Writer");

        // TX-2: reads acc-1 under READ_UNCOMMITTED — sees the dirty value
        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-DirtyReader");
            try { tx1HasWritten.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // READ_UNCOMMITTED: can see uncommitted writes
            Optional<Account> seen = db.readUncommitted("acc-1");
            seen.ifPresent(a -> System.out.printf(
                "[TX2] READ UNCOMMITTED: saw balance=%.2f  <-- DIRTY READ (this data doesn't exist!)%n",
                a.getBalance()));
            tx2HasRead.countDown();
        }, "TX2-DirtyReader");

        tx1.start();
        tx2.start();
        tx1.join();
        tx2.join();

        // Confirm actual committed value
        db.findAccount("acc-1").ifPresent(a ->
            System.out.printf("[RESULT] Actual committed balance=%.2f (TX2 was wrong!)%n", a.getBalance()));
    }

    // -------------------------------------------------------------------------
    // Part B: FIX — READ_COMMITTED never sees uncommitted data
    // -------------------------------------------------------------------------

    public void demonstrateFix() throws InterruptedException {
        System.out.println("\n--- DIRTY READ: FIX (READ_COMMITTED) ---");
        db.reset();

        CountDownLatch tx1HasWritten = new CountDownLatch(1);
        CountDownLatch tx2HasRead    = new CountDownLatch(1);

        // TX-1: same uncommitted write + rollback
        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-Writer");
            Account dirty = db.findAccount("acc-1").orElseThrow();
            dirty.setBalance(0);
            db.writeUncommitted(dirty);
            System.out.println("[TX1] Dirty write buffered (not committed)");
            tx1HasWritten.countDown();

            try { tx2HasRead.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            db.rollbackUncommitted("acc-1");
            System.out.println("[TX1] ROLLBACK complete");
        }, "TX1-Writer");

        // TX-2: READ_COMMITTED — reads the COMMITTED version via snapshotRead
        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-CommittedReader");
            try { tx1HasWritten.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            long snapshotTs = db.beginTx();
            // snapshot read only sees committed versions up to snapshotTs
            Optional<Account> seen = db.snapshotRead("acc-1", snapshotTs);
            seen.ifPresent(a -> System.out.printf(
                "[TX2] READ COMMITTED: saw balance=%.2f  <-- CORRECT (committed value)%n",
                a.getBalance()));
            tx2HasRead.countDown();
        }, "TX2-CommittedReader");

        tx1.start();
        tx2.start();
        tx1.join();
        tx2.join();
    }
}
