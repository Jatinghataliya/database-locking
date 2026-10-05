package com.example.locking.locks;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * READ / WRITE LOCK (Shared vs Exclusive Lock)
 * =============================================
 * Philosophy: "Readers don't block each other — only writers need exclusivity."
 *
 * Lock modes:
 *   READ  LOCK (Shared, S-lock):
 *     - Multiple threads can hold a read lock simultaneously.
 *     - Blocked only when a WRITE lock is held.
 *     - Used for: SELECT queries that must see a consistent snapshot.
 *
 *   WRITE LOCK (Exclusive, X-lock):
 *     - Only ONE thread can hold the write lock at a time.
 *     - Blocked by ANY other read OR write lock.
 *     - Used for: INSERT, UPDATE, DELETE — changes that must be atomic.
 *
 * Compatibility matrix:
 *           | Read held | Write held |
 *   --------|-----------|------------|
 *   Read    |   OK      |  BLOCKED   |
 *   Write   |  BLOCKED  |  BLOCKED   |
 *
 * Real-world SQL equivalent:
 *   LOCK TABLE accounts IN SHARE MODE;          -- read lock
 *   LOCK TABLE accounts IN EXCLUSIVE MODE;      -- write lock
 *   SELECT ... FOR SHARE;                       -- row-level read lock (MySQL 8+)
 *   SELECT ... FOR UPDATE;                      -- row-level write lock
 *
 * Best for:
 *   - Read-heavy systems (news feeds, product catalogs) where most operations are reads.
 *   - Reporting queries that need a consistent snapshot without blocking all writes forever.
 *
 * Worst for:
 *   - Write-heavy workloads (write lock contention defeats the purpose).
 */
public class ReadWriteLockDemo {

    private final InMemoryDatabase db;

    /**
     * Java's ReentrantReadWriteLock mirrors exactly what a DB engine does:
     * - readLock()  = S-lock (shared) — multiple concurrent holders allowed
     * - writeLock() = X-lock (exclusive) — single holder, blocks all others
     */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true); // fair mode
    private final ReentrantReadWriteLock.ReadLock  readLock  = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    public ReadWriteLockDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // READ operation — acquires S-lock (shared)
    // -------------------------------------------------------------------------

    /**
     * Reads an account balance under a shared read lock.
     * Many threads may call this concurrently — they all proceed in parallel
     * as long as no write lock is held.
     */
    public Optional<Account> readAccount(String id) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[RW-LOCK] %s: Acquiring READ (shared) lock...%n", thread);
        readLock.lock();
        try {
            System.out.printf("[RW-LOCK] %s: READ lock ACQUIRED. Active readers=%d%n",
                    thread, rwLock.getReadLockCount());
            Optional<Account> account = db.findAccount(id);
            account.ifPresent(a ->
                System.out.printf("[RW-LOCK] %s: Read account=%s%n", thread, a));
            return account;
        } finally {
            readLock.unlock();
            System.out.printf("[RW-LOCK] %s: READ lock RELEASED.%n", thread);
        }
    }

    // -------------------------------------------------------------------------
    // WRITE operation — acquires X-lock (exclusive)
    // -------------------------------------------------------------------------

    /**
     * Updates an account balance under an exclusive write lock.
     * All concurrent readers AND writers are blocked until this completes.
     */
    public void creditAccount(String id, double amount) {
        String thread = Thread.currentThread().getName();
        System.out.printf("[RW-LOCK] %s: Acquiring WRITE (exclusive) lock for credit...%n", thread);
        writeLock.lock();
        try {
            System.out.printf("[RW-LOCK] %s: WRITE lock ACQUIRED. All readers/writers now blocked.%n", thread);
            Optional<Account> opt = db.findAccount(id);
            if (opt.isEmpty()) return;

            Account account = opt.get();
            account.setBalance(account.getBalance() + amount);
            db.saveAccount(account);
            System.out.printf("[RW-LOCK] %s: Credited %.2f -> %s%n", thread, amount, account);
        } finally {
            writeLock.unlock();
            System.out.printf("[RW-LOCK] %s: WRITE lock RELEASED.%n", thread);
        }
    }

    // -------------------------------------------------------------------------
    // Lock upgrade demo: read -> write
    // -------------------------------------------------------------------------

    /**
     * Demonstrates a conditional upgrade from read to write lock.
     *
     * IMPORTANT: Java's ReentrantReadWriteLock does NOT support direct upgrade
     * (read -> write without releasing). This is intentional — direct upgrades
     * cause deadlocks when two threads try to upgrade simultaneously.
     *
     * The safe pattern is: release read lock, re-acquire write lock, re-read.
     * This is called a "check-then-act" pattern.
     */
    public void conditionalDebit(String id, double amount) {
        String thread = Thread.currentThread().getName();

        // Phase 1: Read under shared lock
        readLock.lock();
        double currentBalance;
        try {
            Optional<Account> opt = db.findAccount(id);
            if (opt.isEmpty()) return;
            currentBalance = opt.get().getBalance();
            System.out.printf("[RW-LOCK] %s: Read balance=%.2f under READ lock.%n", thread, currentBalance);
        } finally {
            readLock.unlock(); // MUST release read lock before acquiring write lock
        }

        // Phase 2: Upgrade to write lock only if balance is sufficient
        if (currentBalance >= amount) {
            writeLock.lock();
            try {
                // Re-read under write lock (balance may have changed since Phase 1)
                Optional<Account> opt = db.findAccount(id);
                if (opt.isEmpty()) return;
                Account account = opt.get();
                if (account.getBalance() >= amount) {
                    account.setBalance(account.getBalance() - amount);
                    db.saveAccount(account);
                    System.out.printf("[RW-LOCK] %s: Debit %.2f SUCCEEDED -> %s%n", thread, amount, account);
                } else {
                    System.out.printf("[RW-LOCK] %s: Balance changed before write lock — debit skipped.%n", thread);
                }
            } finally {
                writeLock.unlock();
            }
        } else {
            System.out.printf("[RW-LOCK] %s: Insufficient balance — write lock not needed.%n", thread);
        }
    }

    public int getReadLockCount()    { return rwLock.getReadLockCount(); }
    public boolean isWriteLocked()   { return rwLock.isWriteLocked(); }
}
