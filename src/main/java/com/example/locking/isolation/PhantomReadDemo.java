package com.example.locking.isolation;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ANOMALY: PHANTOM READ
 * =====================
 * Isolation level where this occurs: REPEATABLE READ (in some engines)
 * Prevented by:                      SERIALIZABLE (or predicate locks / gap locks)
 *
 * What is a Phantom Read?
 *   Transaction-1 executes the same RANGE QUERY twice.
 *   Between the two queries, Transaction-2 INSERTS or DELETES a row that
 *   falls within that range.
 *   Transaction-1's second query returns a DIFFERENT set of rows — a "phantom" row
 *   appears (or disappears) that didn't exist (or did exist) before.
 *
 * Key difference from Non-Repeatable Read:
 *   Non-Repeatable = existing ROW values change (UPDATE).
 *   Phantom         = the SET OF ROWS changes (INSERT / DELETE).
 *
 * Real-world consequence:
 *   - A report TX counts all accounts with balance > $500: finds 2.
 *   - Another TX inserts a new account with balance $750 and commits.
 *   - The report re-counts: finds 3. Totals in the report are now inconsistent.
 *
 * SQL to reproduce (REPEATABLE READ — MySQL allows phantoms):
 *   -- Session 1:
 *   BEGIN;
 *   SELECT COUNT(*) FROM accounts WHERE balance > 500;  -- returns 2
 *
 *   -- Session 2:
 *   INSERT INTO accounts VALUES ('acc-new', 'Dave', 750);
 *   COMMIT;
 *
 *   -- Session 1 (same TX):
 *   SELECT COUNT(*) FROM accounts WHERE balance > 500;  -- returns 3 <- PHANTOM
 *   COMMIT;
 *
 * MySQL InnoDB REPEATABLE READ actually prevents phantoms via "gap locks".
 * PostgreSQL uses MVCC — snapshot reads prevent phantoms at REPEATABLE READ.
 * True SERIALIZABLE adds predicate locks to prevent ALL phantom scenarios.
 */
public class PhantomReadDemo {

    private final InMemoryDatabase db;

    public PhantomReadDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Part A: PROBLEM — REPEATABLE READ (naive): range query result set changes
    // -------------------------------------------------------------------------

    public void demonstrateProblem() throws InterruptedException {
        System.out.println("\n--- PHANTOM READ: PROBLEM (READ_COMMITTED range query) ---");
        db.reset();

        CountDownLatch tx1FirstQuery = new CountDownLatch(1);
        CountDownLatch tx2Inserted   = new CountDownLatch(1);

        // TX-1: runs the same range query twice against the live (non-snapshot) table
        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-RangeReader");

            List<Account> first = db.rangeQuery(500, 3000);
            System.out.printf("[TX1] First range query (balance 500-3000): %d rows -> %s%n",
                    first.size(), first.stream().map(Account::getId).toList());
            tx1FirstQuery.countDown();

            try { tx2Inserted.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // Second query — TX-2's INSERT is now committed and visible
            List<Account> second = db.rangeQuery(500, 3000);
            System.out.printf("[TX1] Second range query (balance 500-3000): %d rows -> %s%n",
                    second.size(), second.stream().map(Account::getId).toList());

            if (second.size() > first.size()) {
                System.out.println("[TX1] PHANTOM confirmed: new row appeared between two identical queries!");
            }
        }, "TX1-RangeReader");

        // TX-2: inserts a new account in the queried range between TX-1's reads
        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Inserter");
            try { tx1FirstQuery.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            Account phantom = new Account("acc-phantom", "Dave", 750.00);
            db.insertAccount(phantom);
            System.out.println("[TX2] INSERT committed: acc-phantom (balance=750) — inside TX1's range!");
            tx2Inserted.countDown();
        }, "TX2-Inserter");

        tx1.start(); tx2.start();
        tx1.join();  tx2.join();
    }

    // -------------------------------------------------------------------------
    // Part B: FIX — SERIALIZABLE: snapshot at BEGIN freezes the result set
    // -------------------------------------------------------------------------

    public void demonstrateFix() throws InterruptedException {
        System.out.println("\n--- PHANTOM READ: FIX (SERIALIZABLE / MVCC snapshot range read) ---");
        db.reset();

        CountDownLatch tx1FirstQuery = new CountDownLatch(1);
        CountDownLatch tx2Inserted   = new CountDownLatch(1);

        // TX-1: takes a snapshot at BEGIN; range queries use that snapshot
        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-SnapshotRange");
            // Snapshot fixed — inserts after this point are invisible
            long snapshotTs = db.beginTx();

            // First range query using snapshot
            List<Account> first = db.rangeQuery(500, 3000); // for simplicity uses live table
            System.out.printf("[TX1] First range query (snapshotTs=%d): %d rows%n",
                    snapshotTs, first.size());
            tx1FirstQuery.countDown();

            try { tx2Inserted.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

            // Snapshot-based count: only count accounts whose version was committed <= snapshotTs
            long count = db.rangeQuery(500, 3000).stream()
                    .filter(a -> db.snapshotRead(a.getId(), snapshotTs).isPresent())
                    .count();
            System.out.printf("[TX1] Second range query (filtered by snapshotTs=%d): %d rows%n",
                    snapshotTs, count);

            if (count == first.size()) {
                System.out.println("[TX1] CORRECT: phantom prevented — insert after snapshot is invisible.");
            }
        }, "TX1-SnapshotRange");

        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Inserter");
            try { tx1FirstQuery.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            db.insertAccount(new Account("acc-phantom", "Dave", 750.00));
            System.out.println("[TX2] INSERT committed: acc-phantom (invisible to TX1's snapshot)");
            tx2Inserted.countDown();
        }, "TX2-Inserter");

        tx1.start(); tx2.start();
        tx1.join();  tx2.join();
    }
}
