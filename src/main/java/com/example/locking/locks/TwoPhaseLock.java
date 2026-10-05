package com.example.locking.locks;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * TWO-PHASE LOCKING (2PL)
 * =======================
 * Philosophy: "Acquire ALL locks you will ever need before releasing ANY of them."
 *
 * The Two-Phase Locking protocol divides a transaction into exactly two phases:
 *
 *   Phase 1 — GROWING (Lock Acquisition):
 *     The transaction acquires locks as it accesses data.
 *     It may NOT release any lock during this phase.
 *
 *   Phase 2 — SHRINKING (Lock Release):
 *     The transaction releases locks one by one.
 *     It may NOT acquire any NEW lock during this phase.
 *
 *   The boundary between the two phases is called the "lock point".
 *
 * Why 2PL guarantees serializability:
 *   No two transactions can have overlapping lock points in a conflicting order.
 *   This means the execution is equivalent to some serial schedule.
 *
 * Variants:
 *   - Basic 2PL          — releases locks before transaction ends (less safe).
 *   - Strict 2PL         — holds ALL locks until COMMIT/ROLLBACK (most common in real DBs).
 *   - Rigorous 2PL       — holds read AND write locks until commit.
 *   - Conservative 2PL   — acquires ALL locks needed upfront before starting work.
 *
 * This demo implements Strict 2PL: all locks are held until the transaction
 * explicitly calls commit() or rollback(), which releases them all at once.
 *
 * Real-world equivalents:
 *   InnoDB (MySQL), PostgreSQL, SQL Server all use 2PL internally.
 *   The BEGIN / COMMIT block in SQL is the user-facing API for Strict 2PL.
 */
public class TwoPhaseLock {

    private final InMemoryDatabase db;

    public TwoPhaseLock(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Transaction object — represents one unit of work
    // -------------------------------------------------------------------------

    /**
     * A Transaction tracks which rows have been locked (growing phase) and
     * releases them all atomically on commit/rollback (shrinking phase).
     *
     * This mirrors what a real DB transaction does internally:
     *   - BEGIN starts the growing phase.
     *   - Every row access acquires the lock for that row.
     *   - COMMIT / ROLLBACK triggers the shrinking phase.
     */
    public class Transaction {
        private final String txId;
        private final List<String> heldLocks = new ArrayList<>();
        private boolean committed = false;

        private Transaction(String txId) {
            this.txId = txId;
            System.out.printf("[2PL] Transaction '%s' STARTED (growing phase begins).%n", txId);
        }

        /**
         * GROWING PHASE: acquire the row lock and remember it.
         * Throws if called after commit/rollback (shrinking phase started).
         */
        public Account lockAndRead(String accountId) {
            if (committed) {
                throw new IllegalStateException("Cannot acquire lock after commit/rollback (shrinking phase).");
            }
            // Acquire row lock — blocks if another tx holds it
            db.lockRow(accountId);
            heldLocks.add(accountId);
            System.out.printf("[2PL] TX '%s' (growing): locked row='%s'. Total locks held=%d%n",
                    txId, accountId, heldLocks.size());

            Optional<Account> opt = db.findAccount(accountId);
            return opt.orElseThrow(() -> new RuntimeException("Account not found: " + accountId));
        }

        /** Write the account — lock must already be held (acquired via lockAndRead). */
        public void write(Account account) {
            if (!heldLocks.contains(account.getId())) {
                throw new IllegalStateException(
                        "Cannot write to row '" + account.getId() + "' without holding its lock.");
            }
            db.saveAccount(account);
            System.out.printf("[2PL] TX '%s': wrote row='%s'.%n", txId, account.getId());
        }

        /**
         * SHRINKING PHASE: release ALL locks at once (Strict 2PL).
         * After this point no new locks may be acquired.
         */
        public void commit() {
            committed = true;
            System.out.printf("[2PL] TX '%s': COMMIT — shrinking phase begins, releasing %d locks.%n",
                    txId, heldLocks.size());
            for (String rowId : new ArrayList<>(heldLocks)) {
                db.unlockRow(rowId);
            }
            heldLocks.clear();
            System.out.printf("[2PL] TX '%s': All locks released.%n", txId);
        }

        public void rollback() {
            committed = true;
            System.out.printf("[2PL] TX '%s': ROLLBACK — releasing %d locks without saving.%n",
                    txId, heldLocks.size());
            for (String rowId : new ArrayList<>(heldLocks)) {
                db.unlockRow(rowId);
            }
            heldLocks.clear();
        }

        public String getTxId() { return txId; }
    }

    /** Factory: begin a new transaction (equivalent to SQL BEGIN). */
    public Transaction begin(String txId) {
        return new Transaction(txId);
    }

    // -------------------------------------------------------------------------
    // Example: Multi-account salary update inside a 2PL transaction
    // -------------------------------------------------------------------------

    /**
     * Credits a salary to multiple accounts within a single Strict 2PL transaction.
     * All rows are locked before any write happens — the transaction holds all
     * locks until it commits, guaranteeing serializability.
     */
    public void batchCredit(String txId, double creditAmount, String... accountIds) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[2PL] %s: Starting batch credit of %.2f to %d accounts.%n",
                thread, creditAmount, accountIds.length);

        Transaction tx = begin(txId);
        try {
            // GROWING PHASE: lock and read all rows
            List<Account> accounts = new ArrayList<>();
            for (String id : accountIds) {
                accounts.add(tx.lockAndRead(id));
            }

            // All locks acquired — now perform writes
            for (Account acc : accounts) {
                acc.setBalance(acc.getBalance() + creditAmount);
                tx.write(acc);
            }

            // SHRINKING PHASE: commit releases all locks
            tx.commit();
            System.out.printf("[2PL] %s: Batch credit SUCCEEDED.%n", thread);

        } catch (Exception e) {
            tx.rollback();
            System.out.printf("[2PL] %s: Batch credit FAILED — rolled back. Reason: %s%n",
                    thread, e.getMessage());
        }
    }
}
