package com.example.locking.locks;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;
import com.example.locking.model.Product;

import java.util.Optional;

/**
 * PESSIMISTIC LOCKING
 * ===================
 * Philosophy: "Conflicts are likely — lock the row before reading it."
 *
 * How it works:
 *   1. LOCK the row before reading (SELECT ... FOR UPDATE in SQL).
 *      Other threads that want the same row are blocked until we release.
 *   2. READ the row — we now have exclusive access; no other thread can modify it.
 *   3. MODIFY and WRITE.
 *   4. UNLOCK (COMMIT / ROLLBACK in SQL releases the lock automatically).
 *
 * Real-world SQL equivalent:
 *   BEGIN;
 *   SELECT * FROM accounts WHERE id = ? FOR UPDATE;   -- acquires row lock
 *   UPDATE accounts SET balance = ? WHERE id = ?;
 *   COMMIT;                                            -- releases lock
 *
 * Best for:
 *   - Write-heavy workloads with high contention (many threads fighting same rows).
 *   - Financial operations where partial writes are catastrophically wrong.
 *   - Short transactions where lock-hold time is bounded and small.
 *
 * Worst for:
 *   - Long-running transactions (locks starve other readers/writers).
 *   - Deadlock-prone if multiple locks are acquired in inconsistent order.
 *   - Distributed / stateless services (lock can't outlive the DB connection).
 */
public class PessimisticLock {

    private final InMemoryDatabase db;

    public PessimisticLock(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Example 1: Bank transfer — row lock on both accounts
    // -------------------------------------------------------------------------

    /**
     * Transfers 'amount' from source to destination using pessimistic row locks.
     *
     * Lock ordering: always lock the lower ID first to prevent deadlocks
     * when two threads transfer in opposite directions simultaneously.
     */
    public boolean transfer(String fromId, String toId, double amount) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[PESSIMISTIC] %s: Starting transfer %.2f from '%s' to '%s'%n",
                thread, amount, fromId, toId);

        // Consistent lock order: alphabetically lower ID locked first
        String first  = fromId.compareTo(toId) < 0 ? fromId : toId;
        String second = fromId.compareTo(toId) < 0 ? toId   : fromId;

        db.lockRow(first);
        db.lockRow(second);
        try {
            // Safe to read now — exclusive access guaranteed
            Optional<Account> fromOpt = db.findAccount(fromId);
            Optional<Account> toOpt   = db.findAccount(toId);

            if (fromOpt.isEmpty() || toOpt.isEmpty()) return false;

            Account from = fromOpt.get();
            Account to   = toOpt.get();

            System.out.printf("[PESSIMISTIC] %s: Locked — from=%s, to=%s%n", thread, from, to);

            if (from.getBalance() < amount) {
                System.out.printf("[PESSIMISTIC] %s: Insufficient balance — rolling back.%n", thread);
                return false;
            }

            from.setBalance(from.getBalance() - amount);
            to.setBalance(to.getBalance() + amount);

            db.saveAccount(from);
            db.saveAccount(to);

            System.out.printf("[PESSIMISTIC] %s: Transfer SUCCEEDED.%n", thread);
            return true;
        } finally {
            // Always release in reverse order (good practice, mirrors real COMMIT)
            db.unlockRow(second);
            db.unlockRow(first);
        }
    }

    // -------------------------------------------------------------------------
    // Example 2: Inventory reservation — lock before checking stock
    // -------------------------------------------------------------------------

    /**
     * Reserves 'qty' units of a product by locking the product row first.
     * No other thread can read or modify this product row until we finish.
     */
    public boolean reserveStock(String productId, int qty) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[PESSIMISTIC] %s: Reserving %d units of product='%s'%n",
                thread, qty, productId);

        db.lockRow(productId);
        try {
            Optional<Product> opt = db.findProduct(productId);
            if (opt.isEmpty()) return false;

            Product product = opt.get();
            System.out.printf("[PESSIMISTIC] %s: Locked product=%s%n", thread, product);

            if (product.getStock() < qty) {
                System.out.printf("[PESSIMISTIC] %s: Insufficient stock — rolling back.%n", thread);
                return false;
            }

            product.setStock(product.getStock() - qty);
            db.saveProduct(product);
            System.out.printf("[PESSIMISTIC] %s: Reservation SUCCEEDED. Remaining stock=%d%n",
                    thread, product.getStock());
            return true;
        } finally {
            db.unlockRow(productId);
        }
    }
}
