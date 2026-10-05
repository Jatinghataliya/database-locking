package com.example.locking.locks;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;
import com.example.locking.model.Product;

import java.util.Optional;

/**
 * OPTIMISTIC LOCKING
 * ==================
 * Philosophy: "Conflicts are rare — read freely, validate before writing."
 *
 * How it works:
 *   1. READ  the row and note its current version number.
 *   2. MODIFY the data in application memory (no lock held on DB).
 *   3. UPDATE with a WHERE clause that includes the original version:
 *        UPDATE accounts SET balance=?, version=version+1
 *        WHERE id=? AND version=<original_version>
 *   4. If 0 rows updated -> another thread changed the row since our read
 *      -> CONFLICT detected -> retry or report error.
 *   5. If 1 row updated -> success, version bumped.
 *
 * Real-world SQL equivalent:
 *   @Version annotation in JPA/Hibernate generates this pattern automatically.
 *
 * Best for:
 *   - Read-heavy workloads where conflicts are uncommon.
 *   - HTTP APIs (stateless; lock cannot span a network request anyway).
 *   - Distributed systems where holding a DB lock across network calls is impossible.
 *
 * Worst for:
 *   - Write-heavy contention (many retries degrade throughput).
 *   - Financial transactions where "last writer wins" is unacceptable without retry logic.
 */
public class OptimisticLock {

    private final InMemoryDatabase db;
    private static final int MAX_RETRIES = 3;

    public OptimisticLock(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Example 1: Bank transfer with retry on conflict
    // -------------------------------------------------------------------------

    /**
     * Transfers 'amount' from the source account to the destination account.
     * Neither account is locked during the read — conflicts are detected at
     * write time via the version field.
     *
     * @return true if the transfer succeeded (within MAX_RETRIES attempts)
     */
    public boolean transfer(String fromId, String toId, double amount) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[OPTIMISTIC] %s: Attempting transfer %.2f from '%s' to '%s'%n",
                thread, amount, fromId, toId);

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            // Step 1: Read snapshots — no lock held
            Optional<Account> fromOpt = db.findAccount(fromId);
            Optional<Account> toOpt   = db.findAccount(toId);

            if (fromOpt.isEmpty() || toOpt.isEmpty()) {
                System.out.printf("[OPTIMISTIC] %s: Account not found.%n", thread);
                return false;
            }

            Account from = fromOpt.get();
            Account to   = toOpt.get();

            System.out.printf("[OPTIMISTIC] %s attempt %d: Read from=%s, to=%s%n",
                    thread, attempt, from, to);

            // Step 2: Validate business rule in application memory
            if (from.getBalance() < amount) {
                System.out.printf("[OPTIMISTIC] %s: Insufficient balance.%n", thread);
                return false;
            }

            // Step 3: Modify in-memory snapshots
            from.setBalance(from.getBalance() - amount);
            to.setBalance(to.getBalance() + amount);

            // Step 4: Try to persist — DB checks version match atomically
            boolean fromOk = db.updateAccountOptimistic(from);
            boolean toOk   = db.updateAccountOptimistic(to);

            if (fromOk && toOk) {
                System.out.printf("[OPTIMISTIC] %s: Transfer SUCCEEDED on attempt %d.%n", thread, attempt);
                return true;
            }

            // Step 5: Conflict — another thread modified one of the rows; retry
            System.out.printf("[OPTIMISTIC] %s: Version conflict on attempt %d — retrying...%n",
                    thread, attempt);
        }

        System.out.printf("[OPTIMISTIC] %s: Transfer FAILED after %d retries.%n", thread, MAX_RETRIES);
        return false;
    }

    // -------------------------------------------------------------------------
    // Example 2: Inventory stock deduction
    // -------------------------------------------------------------------------

    /**
     * Deducts 'qty' units from product stock using optimistic locking.
     * Multiple concurrent order threads racing on the same product will
     * naturally serialize through version conflicts and retries.
     */
    public boolean deductStock(String productId, int qty) {
        String thread = Thread.currentThread().getName();

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            Optional<Product> opt = db.findProduct(productId);
            if (opt.isEmpty()) return false;

            Product product = opt.get();
            System.out.printf("[OPTIMISTIC] %s attempt %d: Read product=%s%n", thread, attempt, product);

            if (product.getStock() < qty) {
                System.out.printf("[OPTIMISTIC] %s: Insufficient stock (%d < %d).%n",
                        thread, product.getStock(), qty);
                return false;
            }

            product.setStock(product.getStock() - qty);

            if (db.updateProductOptimistic(product)) {
                System.out.printf("[OPTIMISTIC] %s: Stock deduction SUCCEEDED (attempt %d). New stock=%d%n",
                        thread, attempt, product.getStock());
                return true;
            }

            System.out.printf("[OPTIMISTIC] %s: Version conflict — retrying...%n", thread);
        }

        System.out.printf("[OPTIMISTIC] %s: Stock deduction FAILED after %d retries.%n", thread, MAX_RETRIES);
        return false;
    }
}
