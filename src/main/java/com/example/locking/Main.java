package com.example.locking;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.locks.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        // Shared DB with 20ms simulated latency
        InMemoryDatabase db = new InMemoryDatabase(20);

        separator("DATABASE LOCKING DEMOS");

        // ================================================================
        // 1. OPTIMISTIC LOCKING
        // ================================================================
        separator("1. OPTIMISTIC LOCKING");
        db.reset();
        OptimisticLock optimistic = new OptimisticLock(db);

        System.out.println("\n--- Single transfer (no conflict) ---");
        optimistic.transfer("acc-1", "acc-2", 200.00);
        db.findAccount("acc-1").ifPresent(a -> System.out.println("After: " + a));
        db.findAccount("acc-2").ifPresent(a -> System.out.println("After: " + a));

        System.out.println("\n--- Concurrent transfers (conflict + retry) ---");
        db.reset();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(1);
        pool.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            optimistic.transfer("acc-1", "acc-2", 100.00);
        });
        pool.submit(() -> {
            try { latch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            optimistic.transfer("acc-1", "acc-3", 150.00);
        });
        latch.countDown();
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);
        db.findAccount("acc-1").ifPresent(a -> System.out.println("acc-1 final: " + a));

        System.out.println("\n--- Inventory stock deduction (concurrent, version conflict) ---");
        db.reset();
        ExecutorService pool2 = Executors.newFixedThreadPool(3);
        CountDownLatch latch2 = new CountDownLatch(1);
        for (int i = 0; i < 3; i++) {
            pool2.submit(() -> {
                try { latch2.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                optimistic.deductStock("p-1", 3);
            });
        }
        latch2.countDown();
        pool2.shutdown();
        pool2.awaitTermination(10, TimeUnit.SECONDS);
        db.findProduct("p-1").ifPresent(p -> System.out.println("p-1 final: " + p));


        // ================================================================
        // 2. PESSIMISTIC LOCKING
        // ================================================================
        separator("2. PESSIMISTIC LOCKING");
        db.reset();
        PessimisticLock pessimistic = new PessimisticLock(db);

        System.out.println("\n--- Single transfer (row lock held) ---");
        pessimistic.transfer("acc-1", "acc-2", 300.00);
        db.findAccount("acc-1").ifPresent(a -> System.out.println("After: " + a));
        db.findAccount("acc-2").ifPresent(a -> System.out.println("After: " + a));

        System.out.println("\n--- Concurrent transfers (threads queue on row locks) ---");
        db.reset();
        ExecutorService pool3 = Executors.newFixedThreadPool(2);
        CountDownLatch latch3 = new CountDownLatch(1);
        pool3.execute(() -> {
            Thread.currentThread().setName("T-Pess-1");
            try { latch3.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            pessimistic.transfer("acc-1", "acc-2", 100.00);
        });
        pool3.execute(() -> {
            Thread.currentThread().setName("T-Pess-2");
            try { latch3.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            pessimistic.transfer("acc-1", "acc-2", 150.00);
        });
        latch3.countDown();
        pool3.shutdown();
        pool3.awaitTermination(10, TimeUnit.SECONDS);
        db.findAccount("acc-1").ifPresent(a -> System.out.println("acc-1 final: " + a));

        System.out.println("\n--- Inventory reservation (exclusive row lock) ---");
        db.reset();
        ExecutorService pool4 = Executors.newFixedThreadPool(3);
        CountDownLatch latch4 = new CountDownLatch(1);
        for (int i = 0; i < 3; i++) {
            pool4.execute(() -> {
                Thread.currentThread().setName("Order-" + Thread.currentThread().getId());
                try { latch4.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                pessimistic.reserveStock("p-2", 8);
            });
        }
        latch4.countDown();
        pool4.shutdown();
        pool4.awaitTermination(10, TimeUnit.SECONDS);
        db.findProduct("p-2").ifPresent(p -> System.out.println("p-2 final: " + p));


        // ================================================================
        // 3. READ / WRITE LOCK
        // ================================================================
        separator("3. READ / WRITE LOCK (Shared vs Exclusive)");
        db.reset();
        ReadWriteLockDemo rwLock = new ReadWriteLockDemo(db);

        System.out.println("\n--- Multiple concurrent readers (all proceed in parallel) ---");
        ExecutorService readers = Executors.newFixedThreadPool(3);
        CountDownLatch rl = new CountDownLatch(1);
        for (int i = 0; i < 3; i++) {
            final int idx = i;
            readers.execute(() -> {
                Thread.currentThread().setName("Reader-" + idx);
                try { rl.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                rwLock.readAccount("acc-1");
            });
        }
        rl.countDown();
        readers.shutdown();
        readers.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n--- Writer blocks all readers (exclusive access) ---");
        db.reset();
        ExecutorService mixed = Executors.newFixedThreadPool(3);
        CountDownLatch ml = new CountDownLatch(1);
        mixed.execute(() -> { Thread.currentThread().setName("Writer-1"); try { ml.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } rwLock.creditAccount("acc-1", 500); });
        mixed.execute(() -> { Thread.currentThread().setName("Reader-A"); try { ml.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } rwLock.readAccount("acc-1"); });
        mixed.execute(() -> { Thread.currentThread().setName("Reader-B"); try { ml.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } rwLock.readAccount("acc-2"); });
        ml.countDown();
        mixed.shutdown();
        mixed.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n--- Conditional read->write upgrade (check-then-act) ---");
        db.reset();
        rwLock.conditionalDebit("acc-2", 100.00);
        db.findAccount("acc-2").ifPresent(a -> System.out.println("After debit: " + a));


        // ================================================================
        // 4. TWO-PHASE LOCKING (2PL)
        // ================================================================
        separator("4. TWO-PHASE LOCKING (2PL — Strict)");
        db.reset();
        TwoPhaseLock tpl = new TwoPhaseLock(db);

        System.out.println("\n--- Batch salary credit (growing then shrinking phase) ---");
        tpl.batchCredit("TX-001", 500.00, "acc-1", "acc-2", "acc-3");
        db.findAccount("acc-1").ifPresent(a -> System.out.println("After: " + a));
        db.findAccount("acc-2").ifPresent(a -> System.out.println("After: " + a));
        db.findAccount("acc-3").ifPresent(a -> System.out.println("After: " + a));

        System.out.println("\n--- Concurrent 2PL transactions (serialized via row locks) ---");
        db.reset();
        ExecutorService tplPool = Executors.newFixedThreadPool(2);
        CountDownLatch tplLatch = new CountDownLatch(1);
        tplPool.execute(() -> {
            Thread.currentThread().setName("TX-A");
            try { tplLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            tpl.batchCredit("TX-A", 100.00, "acc-1", "acc-2");
        });
        tplPool.execute(() -> {
            Thread.currentThread().setName("TX-B");
            try { tplLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            tpl.batchCredit("TX-B", 200.00, "acc-2", "acc-3");
        });
        tplLatch.countDown();
        tplPool.shutdown();
        tplPool.awaitTermination(10, TimeUnit.SECONDS);
        db.findAccount("acc-2").ifPresent(a -> System.out.println("acc-2 final (both credits): " + a));


        // ================================================================
        // 5. DEADLOCK — SIMULATION + PREVENTION
        // ================================================================
        separator("5. DEADLOCK — Simulation and Prevention");
        db.reset();
        DeadlockDemo deadlock = new DeadlockDemo(db);

        System.out.println("\n--- Deadlock SIMULATION (inconsistent lock order) ---");
        System.out.println("NOTE: InMemoryDatabase busy-waits so threads resolve eventually.");
        ExecutorService dlPool = Executors.newFixedThreadPool(2);
        CountDownLatch dlLatch = new CountDownLatch(1);
        dlPool.execute(() -> {
            Thread.currentThread().setName("DL-Thread-1");
            try { dlLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            deadlock.transferUnsafe("acc-1", "acc-2", 100.00); // locks acc-1 first
        });
        dlPool.execute(() -> {
            Thread.currentThread().setName("DL-Thread-2");
            try { dlLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            deadlock.transferUnsafe("acc-2", "acc-1", 50.00);  // locks acc-2 first -> deadlock
        });
        dlLatch.countDown();
        dlPool.shutdown();
        dlPool.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("\n--- Prevention: Lock Ordering (no circular wait possible) ---");
        db.reset();
        ExecutorService safePool = Executors.newFixedThreadPool(2);
        CountDownLatch safeLatch = new CountDownLatch(1);
        safePool.execute(() -> {
            Thread.currentThread().setName("Safe-Thread-1");
            try { safeLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            deadlock.transferSafeOrdering("acc-1", "acc-2", 100.00);
        });
        safePool.execute(() -> {
            Thread.currentThread().setName("Safe-Thread-2");
            try { safeLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            deadlock.transferSafeOrdering("acc-2", "acc-1", 50.00); // same global order
        });
        safeLatch.countDown();
        safePool.shutdown();
        safePool.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("\n--- Prevention: Timeout (abort if lock not acquired in time) ---");
        db.reset();
        deadlock.transferSafeTimeout("acc-1", "acc-2", 200.00, 2000);


        // ================================================================
        // 6. INTENT LOCKS (IS / IX / S / X)
        // ================================================================
        separator("6. INTENT LOCKS (IS / IX / S / X)");
        db.reset();
        IntentLockDemo intent = new IntentLockDemo(db);

        System.out.println("\n--- Row SELECT (IS + S-row): multiple readers proceed concurrently ---");
        ExecutorService intentReaders = Executors.newFixedThreadPool(2);
        CountDownLatch irl = new CountDownLatch(1);
        intentReaders.execute(() -> { Thread.currentThread().setName("IR-1"); try { irl.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } intent.selectRow("acc-1"); });
        intentReaders.execute(() -> { Thread.currentThread().setName("IR-2"); try { irl.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } intent.selectRow("acc-2"); });
        irl.countDown();
        intentReaders.shutdown();
        intentReaders.awaitTermination(5, TimeUnit.SECONDS);

        System.out.println("\n--- Row UPDATE (IX + X-row): writer gets exclusive row access ---");
        db.reset();
        intent.updateRow("acc-1", 9999.00);
        db.findAccount("acc-1").ifPresent(a -> System.out.println("After update: " + a));

        System.out.println("\n--- Full table S-lock (report scan): waits for row IX to clear ---");
        db.reset();
        ExecutorService scanPool = Executors.newFixedThreadPool(2);
        CountDownLatch scanLatch = new CountDownLatch(1);
        scanPool.execute(() -> { Thread.currentThread().setName("Row-Writer"); try { scanLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } intent.updateRow("acc-3", 777.00); });
        scanPool.execute(() -> { Thread.currentThread().setName("Table-Reader"); try { scanLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } intent.fullTableScan(); });
        scanLatch.countDown();
        scanPool.shutdown();
        scanPool.awaitTermination(10, TimeUnit.SECONDS);

        System.out.println("\n--- DDL X-lock (ALTER TABLE): blocks all readers and writers ---");
        db.reset();
        ExecutorService ddlPool = Executors.newFixedThreadPool(2);
        CountDownLatch ddlLatch = new CountDownLatch(1);
        ddlPool.execute(() -> { Thread.currentThread().setName("DDL-Thread"); try { ddlLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } intent.ddlOperation("ADD COLUMN last_updated TIMESTAMP"); });
        ddlPool.execute(() -> { Thread.currentThread().setName("App-Reader"); try { ddlLatch.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } intent.selectRow("acc-1"); });
        ddlLatch.countDown();
        ddlPool.shutdown();
        ddlPool.awaitTermination(10, TimeUnit.SECONDS);

        separator("ALL DEMOS COMPLETE");
    }

    private static void separator(String title) {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("  " + title);
        System.out.println("=".repeat(60));
    }
}
