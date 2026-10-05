package com.example.locking.isolation;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.concurrent.CountDownLatch;

/**
 * ANOMALY: LOST UPDATE
 * ====================
 * Isolation level where this occurs: READ COMMITTED (without FOR UPDATE)
 * Prevented by:                      Pessimistic locking (SELECT FOR UPDATE)
 *                                    OR Optimistic locking (version check)
 *                                    OR REPEATABLE READ with proper conflict detection
 *
 * What is a Lost Update?
 *   Two transactions read the same row, both modify it independently, and
 *   both write back — the second write silently overwrites the first.
 *   One update is "lost" without any error or warning.
 *
 * Real-world consequence:
 *   - Two hotel booking agents both read "1 room available".
 *   - Both confirm bookings and write "0 rooms".
 *   - The room is double-booked. No error was thrown.
 *
 * This demo shows:
 *   A) The PROBLEM  — Two threads read-modify-write without coordination.
 *   B) Fix via PESSIMISTIC — row lock (SELECT FOR UPDATE) serialises writes.
 *   C) Fix via OPTIMISTIC  — version check rejects the stale writer.
 *
 * SQL to reproduce:
 *   -- Session 1:
 *   BEGIN;
 *   SELECT balance FROM accounts WHERE id = 'acc-1';  -- 1000
 *   -- (compute new balance = 1000 + 200 = 1200)
 *
 *   -- Session 2 (concurrent):
 *   BEGIN;
 *   SELECT balance FROM accounts WHERE id = 'acc-1';  -- 1000
 *   UPDATE accounts SET balance = 1000 + 500 = 1500 WHERE id = 'acc-1';
 *   COMMIT;
 *
 *   -- Session 1 (now commits, overwrites session 2's update):
 *   UPDATE accounts SET balance = 1200 WHERE id = 'acc-1';
 *   COMMIT;
 *   -- FINAL balance = 1200 instead of 1700. Session 2's +500 is LOST.
 */
public class LostUpdateDemo {

    private final InMemoryDatabase db;

    public LostUpdateDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Part A: PROBLEM — both threads read-modify-write with no coordination
    // -------------------------------------------------------------------------

    public void demonstrateProblem() throws InterruptedException {
        System.out.println("\n--- LOST UPDATE: PROBLEM (no locking) ---");
        db.reset();

        // Both threads add to acc-1 balance: +200 and +500 -> expected final = 1700
        CountDownLatch bothRead  = new CountDownLatch(2);
        CountDownLatch goWrite   = new CountDownLatch(1);

        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-Add200");
            Account acc = db.findAccount("acc-1").orElseThrow();
            System.out.printf("[TX1] Read balance=%.2f%n", acc.getBalance());
            bothRead.countDown();
            try { goWrite.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            // Applies +200 to the stale snapshot it read earlier
            acc.setBalance(acc.getBalance() + 200);
            db.saveAccount(acc);
            System.out.printf("[TX1] Wrote balance=%.2f (added +200)%n", acc.getBalance());
        }, "TX1-Add200");

        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Add500");
            Account acc = db.findAccount("acc-1").orElseThrow();
            System.out.printf("[TX2] Read balance=%.2f%n", acc.getBalance());
            bothRead.countDown();
            try { goWrite.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            // Applies +500 to the SAME stale snapshot — overwrites TX1's write
            acc.setBalance(acc.getBalance() + 500);
            db.saveAccount(acc);
            System.out.printf("[TX2] Wrote balance=%.2f (added +500)%n", acc.getBalance());
        }, "TX2-Add500");

        tx1.start(); tx2.start();
        bothRead.await();
        goWrite.countDown();       // release both threads to write simultaneously
        tx1.join(); tx2.join();

        db.findAccount("acc-1").ifPresent(a ->
            System.out.printf("[RESULT] Final balance=%.2f | Expected=1700.00 | LOST=%.2f%n",
                    a.getBalance(), 1700.00 - a.getBalance()));
    }

    // -------------------------------------------------------------------------
    // Part B: FIX via PESSIMISTIC (row lock serialises both writes)
    // -------------------------------------------------------------------------

    public void demonstrateFixPessimistic() throws InterruptedException {
        System.out.println("\n--- LOST UPDATE: FIX (Pessimistic row lock) ---");
        db.reset();

        CountDownLatch start = new CountDownLatch(1);

        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-Pess-Add200");
            try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            db.lockRow("acc-1");
            try {
                Account acc = db.findAccount("acc-1").orElseThrow();
                System.out.printf("[TX1] Locked + read balance=%.2f%n", acc.getBalance());
                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                acc.setBalance(acc.getBalance() + 200);
                db.saveAccount(acc);
                System.out.printf("[TX1] Wrote balance=%.2f (added +200)%n", acc.getBalance());
            } finally { db.unlockRow("acc-1"); }
        }, "TX1-Pess-Add200");

        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Pess-Add500");
            try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            db.lockRow("acc-1"); // blocks until TX1 releases
            try {
                Account acc = db.findAccount("acc-1").orElseThrow(); // reads UPDATED value from TX1
                System.out.printf("[TX2] Locked + read balance=%.2f (sees TX1's write)%n", acc.getBalance());
                acc.setBalance(acc.getBalance() + 500);
                db.saveAccount(acc);
                System.out.printf("[TX2] Wrote balance=%.2f (added +500)%n", acc.getBalance());
            } finally { db.unlockRow("acc-1"); }
        }, "TX2-Pess-Add500");

        tx1.start(); tx2.start();
        start.countDown();
        tx1.join(); tx2.join();

        db.findAccount("acc-1").ifPresent(a ->
            System.out.printf("[RESULT] Final balance=%.2f | Expected=1700.00 -> %s%n",
                    a.getBalance(), a.getBalance() == 1700.00 ? "CORRECT" : "WRONG"));
    }

    // -------------------------------------------------------------------------
    // Part C: FIX via OPTIMISTIC (version check detects stale writer)
    // -------------------------------------------------------------------------

    public void demonstrateFixOptimistic() throws InterruptedException {
        System.out.println("\n--- LOST UPDATE: FIX (Optimistic version check) ---");
        db.reset();

        CountDownLatch bothRead = new CountDownLatch(2);
        CountDownLatch goWrite  = new CountDownLatch(1);

        // Version snapshots taken at read time
        final Account[] snap1 = new Account[1];
        final Account[] snap2 = new Account[1];

        Thread tx1 = new Thread(() -> {
            Thread.currentThread().setName("TX1-Opt-Add200");
            snap1[0] = db.findAccount("acc-1").orElseThrow();
            System.out.printf("[TX1] Read balance=%.2f version=%d%n",
                    snap1[0].getBalance(), snap1[0].getVersion());
            bothRead.countDown();
            try { goWrite.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            snap1[0].setBalance(snap1[0].getBalance() + 200);
            boolean ok = db.updateAccountOptimistic(snap1[0]);
            System.out.printf("[TX1] Write +200: %s%n", ok ? "SUCCEEDED" : "REJECTED (version conflict)");
        }, "TX1-Opt-Add200");

        Thread tx2 = new Thread(() -> {
            Thread.currentThread().setName("TX2-Opt-Add500");
            snap2[0] = db.findAccount("acc-1").orElseThrow();
            System.out.printf("[TX2] Read balance=%.2f version=%d%n",
                    snap2[0].getBalance(), snap2[0].getVersion());
            bothRead.countDown();
            try { goWrite.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            // Tiny sleep so TX1 writes first
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            snap2[0].setBalance(snap2[0].getBalance() + 500);
            boolean ok = db.updateAccountOptimistic(snap2[0]);
            System.out.printf("[TX2] Write +500: %s%n", ok ? "SUCCEEDED" : "REJECTED (stale version — must retry)");
            if (!ok) {
                // Retry with fresh read
                Account fresh = db.findAccount("acc-1").orElseThrow();
                fresh.setBalance(fresh.getBalance() + 500);
                db.updateAccountOptimistic(fresh);
                System.out.printf("[TX2] Retry write +500: SUCCEEDED. New balance=%.2f%n", fresh.getBalance());
            }
        }, "TX2-Opt-Add500");

        tx1.start(); tx2.start();
        bothRead.await();
        goWrite.countDown();
        tx1.join(); tx2.join();

        db.findAccount("acc-1").ifPresent(a ->
            System.out.printf("[RESULT] Final balance=%.2f | Expected=1700.00 -> %s%n",
                    a.getBalance(), a.getBalance() == 1700.00 ? "CORRECT" : "WRONG"));
    }
}
