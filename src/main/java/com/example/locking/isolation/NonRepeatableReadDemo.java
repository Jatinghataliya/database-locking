package com.example.locking.isolation;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * ANOMALY: NON-REPEATABLE READ
 * ============================
 * Isolation level where this occurs: READ COMMITTED
 * Prevented by:                      REPEATABLE READ and above
 *
 * What is a Non-Repeatable Read?
 *   Transaction-1 reads the same row TWICE within the same transaction.
 *   Between the two reads, Transaction-2 commits an UPDATE to that row.
 *   Transaction-1's two reads return DIFFERENT values — it cannot "repeat" its read.
 *
 * Real-world consequence:
 *   - A fund transfer TX reads acc-1 balance = $1000 (enough to transfer $800).
 *   - Another TX immediately drains acc-1 to $0 and commits.
 *   - The first TX re-reads acc-1 = $0 before writing — logic breaks.
 *
 * This demo shows:
 *   A) The PROBLEM  — READ_COMMITTED: second read sees the committed update.
 *   B) The FIX      — REPEATABLE_READ: TX holds snapshot; both reads return same value.
 *
 * SQL to reproduce:
 *   -- Session 1 (READ COMMITTED):
 *   BEGIN;
 *   SELECT balance FROM accounts WHERE id = 'acc-1';  -- returns 1000
 *
 *   -- Session 2:
 *   UPDATE accounts SET balance = 0 WHERE id = 'acc-1';
 *   COMMIT;
 *
 *   -- Session 1 (same transaction, second read):
 *   SELECT balance FROM accounts WHERE id = 'acc-1';  -- returns 0  <- NON-REPEATABLE
 *   COMMIT;
 */
public class NonRepeatableReadDemo {

    private final InMemoryDatabase db;

    public NonRepeatableReadDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Part A: PROBLEM — READ_COMMITTED re-reads see updated committed data
    // -------------------------------------------------------------------------

    public void demonstrateProblem() throws InterruptedException {
        System.out.println("\n--- NON-REPEATABLE READ: PROBLEM (READ_COMMITTED) ---");
        db.reset();

        CountDownLatch tx1FirstRead   = new CountDownLatch(1);
        CountDownLatch tx2Committed   = new CountDownLatch(1);

        // TX-1: reads balance twice — at READ_COMMITTED it re-reads the live row each time
        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-ReReader");

            // First read (READ_COMMITTED = always reads current committed value)
            Optional<Account> first = db.findAccount("acc-1");
            first.ifPresent(a -> System.out.printf(
                "[TX1] First read: balance=%.2f%n", a.getBalance()));
            tx1FirstRead.countDown();

            // Wait for TX-2 to commit its update
            try { tx2Committed.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // Second read in SAME transaction — READ_COMMITTED sees the new committed value
            Optional<Account> second = db.findAccount("acc-1");
            second.ifPresent(a -> System.out.printf(
                "[TX1] Second read: balance=%.2f  <-- NON-REPEATABLE (different value!)%n",
                a.getBalance()));

            first.ifPresent(f -> second.ifPresent(s -> {
                if (f.getBalance() != s.getBalance()) {
                    System.out.println("[TX1] ANOMALY CONFIRMED: same TX, same row, different values!");
                }
            }));
        }, "TX1-ReReader");

        // TX-2: updates and commits between TX-1's two reads
        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Updater");
            try { tx1FirstRead.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            Account acc = db.findAccount("acc-1").orElseThrow();
            acc.setBalance(0);
            db.commitVersion(acc);
            System.out.println("[TX2] UPDATE committed: acc-1 balance -> 0");
            tx2Committed.countDown();
        }, "TX2-Updater");

        tx1.start(); tx2.start();
        tx1.join();  tx2.join();
    }

    // -------------------------------------------------------------------------
    // Part B: FIX — REPEATABLE_READ holds a snapshot; second read is unchanged
    // -------------------------------------------------------------------------

    public void demonstrateFix() throws InterruptedException {
        System.out.println("\n--- NON-REPEATABLE READ: FIX (REPEATABLE_READ / MVCC snapshot) ---");
        db.reset();

        CountDownLatch tx1FirstRead = new CountDownLatch(1);
        CountDownLatch tx2Committed = new CountDownLatch(1);

        // TX-1: takes a snapshot at BEGIN; ALL reads use that snapshot timestamp
        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-SnapshotReader");
            long snapshotTs = db.beginTx(); // snapshot fixed at this point in time

            Optional<Account> first = db.snapshotRead("acc-1", snapshotTs);
            first.ifPresent(a -> System.out.printf(
                "[TX1] First read (snapshot ts=%d): balance=%.2f%n", snapshotTs, a.getBalance()));
            tx1FirstRead.countDown();

            try { tx2Committed.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // Second read uses SAME snapshotTs — TX2's committed version has a higher ts, stays invisible
            Optional<Account> second = db.snapshotRead("acc-1", snapshotTs);
            second.ifPresent(a -> System.out.printf(
                "[TX1] Second read (snapshot ts=%d): balance=%.2f  <-- REPEATABLE (same value)%n",
                snapshotTs, a.getBalance()));

            first.ifPresent(f -> second.ifPresent(s -> {
                if (f.getBalance() == s.getBalance()) {
                    System.out.println("[TX1] CORRECT: both reads returned the same value (snapshot isolation).");
                }
            }));
        }, "TX1-SnapshotReader");

        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Updater");
            try { tx1FirstRead.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            Account acc = db.findAccount("acc-1").orElseThrow();
            acc.setBalance(0);
            db.commitVersion(acc);
            System.out.println("[TX2] UPDATE committed: acc-1 balance -> 0 (invisible to TX1's snapshot)");
            tx2Committed.countDown();
        }, "TX2-Updater");

        tx1.start(); tx2.start();
        tx1.join();  tx2.join();
    }
}
