package com.example.locking.propagation;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.Optional;

/**
 * TRANSACTION PROPAGATION
 * =======================
 * Propagation defines what happens when one transactional method calls another:
 * should the callee JOIN the caller's transaction, START a new one, or refuse
 * to participate at all?
 *
 * This is primarily a Spring @Transactional concept, but the underlying database
 * behaviour is universal (savepoints, nested transactions, connection reuse).
 *
 * Propagation types demonstrated:
 *
 *   REQUIRED      — join existing TX; create new one if none exists (default)
 *   REQUIRES_NEW  — always suspend the outer TX and start a fresh one
 *   NESTED        — run inside a savepoint within the outer TX (can rollback independently)
 *   MANDATORY     — must have an existing TX or throw exception
 *   SUPPORTS      — join if TX exists; proceed without one if not
 *   NOT_SUPPORTED — suspend any TX; run non-transactionally
 *   NEVER         — throw if a TX exists
 *
 * Since this project has no Spring/JPA, we simulate the behaviour explicitly
 * using a lightweight TransactionContext object that mimics what Spring's
 * TransactionManager does internally.
 */
public class PropagationDemo {

    private final InMemoryDatabase db;

    // -------------------------------------------------------------------------
    // Simulated Transaction Context
    // -------------------------------------------------------------------------

    /**
     * Represents the "current transaction" on the calling thread.
     * Spring stores this in a ThreadLocal<TransactionStatus>.
     * Here we pass it explicitly for clarity.
     */
    public static class TxContext {
        public final String txId;
        public boolean rolledBack = false;
        private boolean active = true;

        // Savepoint stack for NESTED propagation
        private Account savedPoint = null;

        public TxContext(String txId) {
            this.txId = txId;
            System.out.printf("[TX] BEGIN transaction '%s'%n", txId);
        }

        public boolean isActive() { return active; }

        public void commit() {
            if (!active) throw new IllegalStateException("TX already closed");
            active = false;
            System.out.printf("[TX] COMMIT transaction '%s'%n", txId);
        }

        public void rollback() {
            if (!active) throw new IllegalStateException("TX already closed");
            rolledBack = true;
            active     = false;
            System.out.printf("[TX] ROLLBACK transaction '%s'%n", txId);
        }

        /** NESTED: create a savepoint (snapshot of an account). */
        public void savepoint(Account account) {
            savedPoint = account.copy();
            System.out.printf("[TX] SAVEPOINT created in '%s' for acc='%s'%n", txId, account.getId());
        }

        /** NESTED: rollback to savepoint — restores account to snapshot. */
        public Account rollbackToSavepoint() {
            if (savedPoint == null) throw new IllegalStateException("No savepoint set");
            System.out.printf("[TX] ROLLBACK TO SAVEPOINT in '%s'%n", txId);
            return savedPoint.copy();
        }
    }

    public PropagationDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // 1. REQUIRED (default) — join existing, or create new
    // -------------------------------------------------------------------------

    /**
     * Caller has an existing TX. The callee JOINS it.
     * If callee throws, the ENTIRE outer TX is rolled back.
     */
    public void demoRequired() {
        System.out.println("\n=== PROPAGATION: REQUIRED ===");
        db.reset();

        TxContext outerTx = new TxContext("OUTER-REQUIRED");

        try {
            // Outer TX: debit acc-1
            Account acc1 = db.findAccount("acc-1").orElseThrow();
            acc1.setBalance(acc1.getBalance() - 200);
            db.saveAccount(acc1);
            System.out.printf("[OUTER] Debited acc-1: %.2f%n", acc1.getBalance());

            // Call inner method — REQUIRED: joins outerTx (no new TX started)
            innerRequired(outerTx, "acc-2", 200);

            outerTx.commit();
            System.out.println("[REQUIRED] Both operations committed in the SAME transaction.");
        } catch (Exception e) {
            outerTx.rollback();
            System.out.println("[REQUIRED] Outer TX rolled back due to: " + e.getMessage());
        }
    }

    private void innerRequired(TxContext tx, String accId, double amount) {
        System.out.printf("[INNER REQUIRED] Joining existing TX '%s'%n", tx.txId);
        Account acc = db.findAccount(accId).orElseThrow();
        acc.setBalance(acc.getBalance() + amount);
        db.saveAccount(acc);
        System.out.printf("[INNER REQUIRED] Credited %s: %.2f%n", accId, acc.getBalance());
        // Any exception here propagates to and rolls back the OUTER TX as well
    }

    // -------------------------------------------------------------------------
    // 2. REQUIRES_NEW — always start a fresh, independent TX
    // -------------------------------------------------------------------------

    /**
     * The inner method SUSPENDS the outer TX and opens its own.
     * The inner TX commits independently — even if the outer TX later rolls back,
     * the inner TX's changes are permanent.
     *
     * Use case: audit log writes that must persist even if the main TX fails.
     */
    public void demoRequiresNew() {
        System.out.println("\n=== PROPAGATION: REQUIRES_NEW ===");
        db.reset();

        TxContext outerTx = new TxContext("OUTER-REQUIRES-NEW");

        try {
            Account acc1 = db.findAccount("acc-1").orElseThrow();
            acc1.setBalance(acc1.getBalance() - 500);
            db.saveAccount(acc1);
            System.out.printf("[OUTER] Debited acc-1: %.2f%n", acc1.getBalance());

            // Inner runs in its OWN transaction — outer is suspended
            innerRequiresNew("acc-2", 500);

            // Simulate outer TX failure AFTER inner already committed
            throw new RuntimeException("Outer TX failed after inner committed!");

        } catch (Exception e) {
            outerTx.rollback();
            System.out.println("[OUTER] Outer TX ROLLED BACK: " + e.getMessage());
        }

        // Inner TX committed its write permanently — acc-2 keeps the credit
        db.findAccount("acc-2").ifPresent(a ->
            System.out.printf("[RESULT] acc-2 balance=%.2f (inner TX committed independently)%n",
                    a.getBalance()));
    }

    private void innerRequiresNew(String accId, double amount) {
        // REQUIRES_NEW: suspend outer TX, open brand-new connection
        TxContext innerTx = new TxContext("INNER-REQUIRES-NEW");
        try {
            System.out.printf("[INNER REQUIRES_NEW] Running in own TX '%s' (outer suspended)%n",
                    innerTx.txId);
            Account acc = db.findAccount(accId).orElseThrow();
            acc.setBalance(acc.getBalance() + amount);
            db.saveAccount(acc);
            System.out.printf("[INNER REQUIRES_NEW] Credited %s: %.2f%n", accId, acc.getBalance());
            innerTx.commit(); // permanently committed, independent of outer
        } catch (Exception e) {
            innerTx.rollback();
        }
    }

    // -------------------------------------------------------------------------
    // 3. NESTED — savepoint within the outer TX
    // -------------------------------------------------------------------------

    /**
     * Inner runs within the SAME outer TX but wrapped in a savepoint.
     * If the inner fails, only the inner's work is rolled back (to the savepoint).
     * The outer TX can still commit its other changes.
     *
     * Use case: try a bonus discount; if it fails, still process the base order.
     */
    public void demoNested() {
        System.out.println("\n=== PROPAGATION: NESTED ===");
        db.reset();

        TxContext outerTx = new TxContext("OUTER-NESTED");

        try {
            // Outer: credit acc-1 (main work)
            Account acc1 = db.findAccount("acc-1").orElseThrow();
            acc1.setBalance(acc1.getBalance() + 100);
            db.saveAccount(acc1);
            System.out.printf("[OUTER] Credited acc-1: %.2f%n", acc1.getBalance());

            // Nested inner: try to apply a bonus to acc-3 — may fail
            try {
                innerNested(outerTx, "acc-3", -9999); // negative amount = simulated failure
            } catch (Exception e) {
                System.out.printf("[OUTER] Inner NESTED failed (%s) — rolled back to savepoint. Outer continues.%n",
                        e.getMessage());
                // Restore acc-3 to its savepoint snapshot
                Account restored = outerTx.rollbackToSavepoint();
                db.saveAccount(restored);
            }

            outerTx.commit();
            System.out.println("[NESTED] Outer TX committed. acc-1 credit saved; acc-3 rolled back to savepoint.");
        } catch (Exception e) {
            outerTx.rollback();
        }

        db.findAccount("acc-1").ifPresent(a -> System.out.printf("[RESULT] acc-1=%.2f%n", a.getBalance()));
        db.findAccount("acc-3").ifPresent(a -> System.out.printf("[RESULT] acc-3=%.2f (savepoint restored)%n", a.getBalance()));
    }

    private void innerNested(TxContext tx, String accId, double amount) {
        Account acc = db.findAccount(accId).orElseThrow();
        tx.savepoint(acc); // create savepoint before inner modifies the row

        System.out.printf("[INNER NESTED] Applying bonus %.2f to %s%n", amount, accId);
        if (amount < 0) {
            throw new RuntimeException("Invalid bonus amount: " + amount);
        }
        acc.setBalance(acc.getBalance() + amount);
        db.saveAccount(acc);
    }

    // -------------------------------------------------------------------------
    // 4. MANDATORY — must have an active TX or throw
    // -------------------------------------------------------------------------

    public void demoMandatory() {
        System.out.println("\n=== PROPAGATION: MANDATORY ===");
        db.reset();

        // Case A: called WITH an outer TX — OK
        System.out.println("[MANDATORY] Case A: called within an existing TX");
        TxContext tx = new TxContext("OUTER-MANDATORY");
        try {
            innerMandatory(tx, "acc-1");
            tx.commit();
        } catch (Exception e) {
            tx.rollback();
        }

        // Case B: called WITHOUT any TX — throws IllegalStateException
        System.out.println("\n[MANDATORY] Case B: called WITHOUT any TX");
        try {
            innerMandatory(null, "acc-1");
        } catch (IllegalStateException e) {
            System.out.println("[MANDATORY] Correctly threw: " + e.getMessage());
        }
    }

    private void innerMandatory(TxContext tx, String accId) {
        if (tx == null || !tx.isActive()) {
            throw new IllegalStateException("MANDATORY: no active transaction found — cannot proceed.");
        }
        System.out.printf("[INNER MANDATORY] Running inside TX '%s' — OK%n", tx.txId);
        Optional<Account> acc = db.findAccount(accId);
        acc.ifPresent(a -> System.out.printf("[INNER MANDATORY] Read %s%n", a));
    }

    // -------------------------------------------------------------------------
    // 5. SUPPORTS — joins TX if exists, runs without if not
    // -------------------------------------------------------------------------

    public void demoSupports() {
        System.out.println("\n=== PROPAGATION: SUPPORTS ===");
        db.reset();

        System.out.println("[SUPPORTS] Case A: called WITH a TX");
        TxContext tx = new TxContext("OUTER-SUPPORTS");
        innerSupports(tx, "acc-1");
        tx.commit();

        System.out.println("\n[SUPPORTS] Case B: called WITHOUT a TX (runs non-transactionally)");
        innerSupports(null, "acc-2");
    }

    private void innerSupports(TxContext tx, String accId) {
        if (tx != null && tx.isActive()) {
            System.out.printf("[INNER SUPPORTS] Joining TX '%s'%n", tx.txId);
        } else {
            System.out.println("[INNER SUPPORTS] No TX present — running non-transactionally (OK)");
        }
        db.findAccount(accId).ifPresent(a -> System.out.printf("[INNER SUPPORTS] Read %s%n", a));
    }

    // -------------------------------------------------------------------------
    // 6. NOT_SUPPORTED — suspends any TX; runs outside transaction
    // -------------------------------------------------------------------------

    public void demoNotSupported() {
        System.out.println("\n=== PROPAGATION: NOT_SUPPORTED ===");
        db.reset();

        TxContext tx = new TxContext("OUTER-NOT-SUPPORTED");
        System.out.printf("[OUTER] Active TX '%s' — calling NOT_SUPPORTED inner%n", tx.txId);
        innerNotSupported(tx, "acc-1");
        tx.commit();
    }

    private void innerNotSupported(TxContext suspendedTx, String accId) {
        System.out.printf("[INNER NOT_SUPPORTED] Suspending TX '%s' — running NON-transactionally%n",
                suspendedTx.txId);
        // Runs without a transaction — changes are auto-committed immediately
        db.findAccount(accId).ifPresent(a ->
            System.out.printf("[INNER NOT_SUPPORTED] Read %s (no TX context)%n", a));
        System.out.printf("[INNER NOT_SUPPORTED] Done — TX '%s' resumes%n", suspendedTx.txId);
    }

    // -------------------------------------------------------------------------
    // 7. NEVER — throws if ANY TX is active
    // -------------------------------------------------------------------------

    public void demoNever() {
        System.out.println("\n=== PROPAGATION: NEVER ===");
        db.reset();

        System.out.println("[NEVER] Case A: called WITHOUT a TX — OK");
        innerNever(null, "acc-1");

        System.out.println("\n[NEVER] Case B: called WITH a TX — throws");
        TxContext tx = new TxContext("OUTER-NEVER");
        try {
            innerNever(tx, "acc-1");
        } catch (IllegalStateException e) {
            System.out.println("[NEVER] Correctly threw: " + e.getMessage());
        } finally {
            tx.rollback();
        }
    }

    private void innerNever(TxContext tx, String accId) {
        if (tx != null && tx.isActive()) {
            throw new IllegalStateException("NEVER: a transaction is active — this method must not run inside a TX.");
        }
        System.out.printf("[INNER NEVER] No TX present — running safely%n");
        db.findAccount(accId).ifPresent(a -> System.out.printf("[INNER NEVER] Read %s%n", a));
    }
}
