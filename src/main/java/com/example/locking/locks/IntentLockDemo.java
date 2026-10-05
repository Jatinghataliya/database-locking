package com.example.locking.locks;

import com.example.locking.db.InMemoryDatabase;
import com.example.locking.model.Account;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * INTENT LOCKS (IS / IX / S / X)
 * ================================
 * Intent locks are used by real database engines (InnoDB, SQL Server, DB2) to
 * efficiently coordinate between ROW-level and TABLE-level locks.
 *
 * The problem they solve:
 *   Without intent locks, to acquire a table-level shared lock (S), the DB would
 *   have to scan EVERY row to check if any row has an exclusive (X) lock on it.
 *   That is O(n) per lock request — catastrophically slow on large tables.
 *
 * The solution — announce your intentions at the table level first:
 *   Before locking a ROW for reading  -> acquire IS (Intent Shared) on the TABLE.
 *   Before locking a ROW for writing  -> acquire IX (Intent Exclusive) on the TABLE.
 *   Before locking the WHOLE TABLE for reading  -> acquire S on the TABLE.
 *   Before locking the WHOLE TABLE for writing  -> acquire X on the TABLE.
 *
 * Compatibility matrix (rows = held, cols = requested):
 *
 *         IS    IX    S     X
 *   IS  [ OK   OK    OK   WAIT ]
 *   IX  [ OK   OK   WAIT  WAIT ]
 *   S   [ OK  WAIT   OK   WAIT ]
 *   X   [WAIT  WAIT  WAIT  WAIT ]
 *
 * Key insight:
 *   IS + IX are compatible -> many row-level readers AND writers can co-exist.
 *   S  + IX are incompatible -> table-level reader blocks row-level writers.
 *   X  blocks everything     -> full table write is completely exclusive.
 *
 * Real-world analogy:
 *   Walking into a library (IS/IX = "I intend to use something inside").
 *   Closing the library for renovation (X = "nobody can enter or read").
 *   Making the library read-only for the day (S = "only readers, no modifications").
 */
public class IntentLockDemo {

    public enum LockMode { IS, IX, S, X }

    private final InMemoryDatabase db;

    // Table-level lock state
    private LockMode tableLockMode = null;
    private String   tableLockOwner = null;

    // Row-level lock state: rowId -> (mode, ownerThread)
    private final Map<String, LockMode> rowLockModes  = new HashMap<>();
    private final Map<String, String>   rowLockOwners = new HashMap<>();

    // Compatibility table
    private static final boolean[][] COMPATIBLE = {
        //       IS      IX      S       X       (requested)
        /* IS */ { true,  true,  true,  false },
        /* IX */ { true,  true,  false, false },
        /* S  */ { true,  false, true,  false },
        /* X  */ { false, false, false, false }
    };

    public IntentLockDemo(InMemoryDatabase db) {
        this.db = db;
    }

    // -------------------------------------------------------------------------
    // Table-level lock acquisition
    // -------------------------------------------------------------------------

    private synchronized void acquireTableLock(LockMode requested) {
        String thread = Thread.currentThread().getName();
        while (tableLockMode != null && !isCompatible(tableLockMode, requested)) {
            System.out.printf("[INTENT] %s: Table lock WAITING (held=%s, requested=%s)%n",
                    thread, tableLockMode, requested);
            try { wait(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        tableLockMode  = requested;
        tableLockOwner = thread;
        System.out.printf("[INTENT] %s: Table lock ACQUIRED as %s%n", thread, requested);
    }

    private synchronized void releaseTableLock() {
        String thread = Thread.currentThread().getName();
        System.out.printf("[INTENT] %s: Table lock RELEASED (was %s)%n", thread, tableLockMode);
        tableLockMode  = null;
        tableLockOwner = null;
        notifyAll();
    }

    // -------------------------------------------------------------------------
    // Row-level lock acquisition
    // -------------------------------------------------------------------------

    private synchronized void acquireRowLock(String rowId, LockMode requested) {
        String thread = Thread.currentThread().getName();
        while (rowLockModes.containsKey(rowId)
                && !rowLockOwners.get(rowId).equals(thread)
                && !isCompatible(rowLockModes.get(rowId), requested)) {
            System.out.printf("[INTENT] %s: Row lock WAITING on '%s' (held=%s, requested=%s)%n",
                    thread, rowId, rowLockModes.get(rowId), requested);
            try { wait(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        rowLockModes.put(rowId, requested);
        rowLockOwners.put(rowId, thread);
        System.out.printf("[INTENT] %s: Row lock ACQUIRED on '%s' as %s%n", thread, rowId, requested);
    }

    private synchronized void releaseRowLock(String rowId) {
        rowLockModes.remove(rowId);
        rowLockOwners.remove(rowId);
        System.out.printf("[INTENT] %s: Row lock RELEASED on '%s'%n",
                Thread.currentThread().getName(), rowId);
        notifyAll();
    }

    private boolean isCompatible(LockMode held, LockMode requested) {
        return COMPATIBLE[held.ordinal()][requested.ordinal()];
    }

    // -------------------------------------------------------------------------
    // Example A: Row-level SELECT (IS + S-row)
    // -------------------------------------------------------------------------

    /**
     * Reads a single row.
     * Protocol: acquire IS on table -> acquire S on row -> read -> release both.
     * Other row readers can proceed in parallel (IS+IS = compatible).
     */
    public Optional<Account> selectRow(String accountId) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[INTENT] %s: SELECT row '%s' -> acquiring IS on table, then S on row%n",
                thread, accountId);

        acquireTableLock(LockMode.IS);
        acquireRowLock(accountId, LockMode.S);
        try {
            Optional<Account> acc = db.findAccount(accountId);
            acc.ifPresent(a -> System.out.printf("[INTENT] %s: Read %s%n", thread, a));
            return acc;
        } finally {
            releaseRowLock(accountId);
            releaseTableLock();
        }
    }

    // -------------------------------------------------------------------------
    // Example B: Row-level UPDATE (IX + X-row)
    // -------------------------------------------------------------------------

    /**
     * Updates a single row.
     * Protocol: acquire IX on table -> acquire X on row -> write -> release both.
     * Other row-level writers are allowed concurrently (IX+IX = compatible),
     * but a full table S-lock would be blocked by our IX.
     */
    public void updateRow(String accountId, double newBalance) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[INTENT] %s: UPDATE row '%s' -> acquiring IX on table, then X on row%n",
                thread, accountId);

        acquireTableLock(LockMode.IX);
        acquireRowLock(accountId, LockMode.X);
        try {
            db.findAccount(accountId).ifPresent(acc -> {
                acc.setBalance(newBalance);
                db.saveAccount(acc);
                System.out.printf("[INTENT] %s: Updated row '%s' balance to %.2f%n",
                        thread, accountId, newBalance);
            });
        } finally {
            releaseRowLock(accountId);
            releaseTableLock();
        }
    }

    // -------------------------------------------------------------------------
    // Example C: Full table SELECT (S-lock) — blocked by IX
    // -------------------------------------------------------------------------

    /**
     * Full table scan (e.g. SELECT * for a report).
     * Protocol: acquire S on table.
     * Incompatible with IX (row writers) — must wait until all row writes finish.
     */
    public void fullTableScan() {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[INTENT] %s: FULL TABLE SCAN -> acquiring S on table%n", thread);

        acquireTableLock(LockMode.S);
        try {
            System.out.printf("[INTENT] %s: Scanning all rows under S-lock (no row writers allowed).%n", thread);
            // Simulate scanning all accounts
            for (String id : new String[]{"acc-1", "acc-2", "acc-3"}) {
                db.findAccount(id).ifPresent(a ->
                    System.out.printf("[INTENT] %s: Scanned %s%n", thread, a));
            }
        } finally {
            releaseTableLock();
        }
    }

    // -------------------------------------------------------------------------
    // Example D: DDL / ALTER TABLE (X-lock) — blocks everything
    // -------------------------------------------------------------------------

    /**
     * Simulates a DDL operation (ALTER TABLE, CREATE INDEX).
     * Protocol: acquire X on table — blocks ALL readers and writers.
     */
    public void ddlOperation(String description) {
        String thread = Thread.currentThread().getName();
        System.out.printf("%n[INTENT] %s: DDL '%s' -> acquiring X on table (blocks ALL)%n",
                thread, description);

        acquireTableLock(LockMode.X);
        try {
            System.out.printf("[INTENT] %s: Executing DDL '%s' under X-lock.%n", thread, description);
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            System.out.printf("[INTENT] %s: DDL '%s' COMPLETE.%n", thread, description);
        } finally {
            releaseTableLock();
        }
    }
}
