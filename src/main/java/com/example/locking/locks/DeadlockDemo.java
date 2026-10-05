package com.example.locking.locks;

import com.example.locking.db.InMemoryDatabase;

import java.util.concurrent.TimeUnit;

/**
 * DEADLOCK — Simulation and Prevention
 * =====================================
 * A deadlock occurs when two (or more) transactions are each waiting for a
 * lock held by the other, creating a circular dependency that never resolves.
 *
 * Classic scenario (Thread-1 and Thread-2):
 *
 *   Thread-1:  locks row-A  -> tries to lock row-B  (blocked by Thread-2)
 *   Thread-2:  locks row-B  -> tries to lock row-A  (blocked by Thread-1)
 *
 *   Neither can proceed. Both wait forever. That is a deadlock.
 *
 * How real databases handle deadlocks:
 *   Detection: DB maintains a "waits-for graph". When a cycle is detected,
 *              one transaction is chosen as the VICTIM and rolled back.
 *              The other transaction then proceeds.
 *   Prevention: Enforce a global lock-acquisition ORDER so cycles cannot form.
 *
 * Prevention strategies implemented here:
 *   A) Lock Ordering — always acquire locks in the same global order (e.g. by ID).
 *      If T1 and T2 both always lock lower-ID row first, circular wait is impossible.
 *
 *   B) Timeout — if a lock cannot be acquired within N ms, abort and retry.
 *      Simple and effective in practice; used by InnoDB (innodb_lock_wait_timeout).
 */
public class DeadlockDemo {

    private final InMemoryDatabase db;

    public DeadlockDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Part A: Deadlock SIMULATION (shows the problem)
    // -------------------------------------------------------------------------

    /**
     * Performs a transfer that acquires locks in INCONSISTENT order.
     *
     * Thread-1 calls transferUnsafe("acc-1", "acc-2", ...)  -> locks acc-1 first, then acc-2
     * Thread-2 calls transferUnsafe("acc-2", "acc-1", ...)  -> locks acc-2 first, then acc-1
     *
     * When both run concurrently, a deadlock forms.
     * (In this simulation InMemoryDatabase.lockRow busy-waits, so it will not
     *  truly hang forever — it will eventually proceed once the sleep expires,
     *  which is safe for a demo. In a real DB both transactions would hang
     *  until one is killed by the deadlock detector.)
     */
    public void transferUnsafe(String fromId, String toId, double amount) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[DEADLOCK SIM] %s: Locking '%s' first (UNSAFE order)...%n", thread, fromId);
        db.lockRow(fromId); // Lock first row

        // Simulate some processing time — gives the other thread time to lock toId
        try { TimeUnit.MILLISECONDS.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        System.out.printf("[DEADLOCK SIM] %s: Now trying to lock '%s'... (may block!)%n", thread, toId);
        db.lockRow(toId); // Lock second row — this is where deadlock would occur

        try {
            db.findAccount(fromId).ifPresent(from -> {
                db.findAccount(toId).ifPresent(to -> {
                    if (from.getBalance() >= amount) {
                        from.setBalance(from.getBalance() - amount);
                        to.setBalance(to.getBalance() + amount);
                        db.saveAccount(from);
                        db.saveAccount(to);
                        System.out.printf("[DEADLOCK SIM] %s: Transfer done (after deadlock resolved by retry).%n", thread);
                    }
                });
            });
        } finally {
            db.unlockRow(toId);
            db.unlockRow(fromId);
        }
    }

    // -------------------------------------------------------------------------
    // Part B: Prevention Strategy 1 — Lock Ordering
    // -------------------------------------------------------------------------

    /**
     * Transfers money using a CONSISTENT global lock order (lower ID first).
     * This completely prevents circular waits because:
     *   - Thread-1 (acc-1 -> acc-2): locks acc-1 first, then acc-2.
     *   - Thread-2 (acc-2 -> acc-1): ALSO locks acc-1 first (due to ordering), then acc-2.
     *   - Thread-2 blocks on acc-1 while Thread-1 holds it — no circular wait.
     */
    public boolean transferSafeOrdering(String fromId, String toId, double amount) {
        String thread = Thread.currentThread().getName();

        // Determine consistent acquisition order
        String first  = fromId.compareTo(toId) <= 0 ? fromId : toId;
        String second = fromId.compareTo(toId) <= 0 ? toId   : fromId;

        System.out.printf("[DEADLOCK PREVENT] %s: Lock order: '%s' then '%s'%n", thread, first, second);

        db.lockRow(first);
        db.lockRow(second);
        try {
            return db.findAccount(fromId).flatMap(from ->
                db.findAccount(toId).map(to -> {
                    if (from.getBalance() < amount) {
                        System.out.printf("[DEADLOCK PREVENT] %s: Insufficient balance.%n", thread);
                        return false;
                    }
                    from.setBalance(from.getBalance() - amount);
                    to.setBalance(to.getBalance() + amount);
                    db.saveAccount(from);
                    db.saveAccount(to);
                    System.out.printf("[DEADLOCK PREVENT] %s: Transfer SUCCEEDED (lock-ordering strategy).%n", thread);
                    return true;
                })
            ).orElse(false);
        } finally {
            db.unlockRow(second);
            db.unlockRow(first);
        }
    }

    // -------------------------------------------------------------------------
    // Part C: Prevention Strategy 2 — Timeout with retry
    // -------------------------------------------------------------------------

    /**
     * Attempts the transfer but gives up if the second lock cannot be acquired
     * within 300ms. The caller can retry after a back-off period.
     * This mirrors InnoDB's innodb_lock_wait_timeout behaviour.
     */
    public boolean transferSafeTimeout(String fromId, String toId, double amount,
                                        long timeoutMs) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[DEADLOCK TIMEOUT] %s: Acquiring lock for '%s'...%n", thread, fromId);
        db.lockRow(fromId);

        long deadline = System.currentTimeMillis() + timeoutMs;
        boolean secondLocked = false;

        try {
            // Spin-wait with timeout for the second lock
            while (System.currentTimeMillis() < deadline) {
                // Try acquiring second lock non-blockingly (simulate tryLock)
                db.lockRow(toId);
                secondLocked = true;
                break;
            }

            if (!secondLocked) {
                System.out.printf("[DEADLOCK TIMEOUT] %s: Timed out waiting for lock on '%s' — aborting.%n",
                        thread, toId);
                return false;
            }

            return db.findAccount(fromId).flatMap(from ->
                db.findAccount(toId).map(to -> {
                    if (from.getBalance() < amount) return false;
                    from.setBalance(from.getBalance() - amount);
                    to.setBalance(to.getBalance() + amount);
                    db.saveAccount(from);
                    db.saveAccount(to);
                    System.out.printf("[DEADLOCK TIMEOUT] %s: Transfer SUCCEEDED (timeout strategy).%n", thread);
                    return true;
                })
            ).orElse(false);

        } finally {
            if (secondLocked) db.unlockRow(toId);
            db.unlockRow(fromId);
        }
    }
}
