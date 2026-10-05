package com.example.locking.db;

import com.example.locking.model.Account;
import com.example.locking.model.Product;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * InMemoryDatabase — simulates a relational database engine.
 *
 * Provides:
 *   - Storage for Account and Product rows (ConcurrentHashMap = heap table)
 *   - Row-level exclusive locks   (pessimistic locking simulation)
 *   - Table-level exclusive lock  (full table lock simulation)
 *   - MVCC snapshot reads         (isolation level simulation)
 *   - Write-ahead uncommitted log (dirty read simulation)
 *   - Configurable simulated DB latency
 *
 * This is NOT a real database. It exists purely to make the locking
 * demos observable and runnable without any external infrastructure.
 */
public class InMemoryDatabase {

    // -------------------------------------------------------------------------
    // Row storage
    // -------------------------------------------------------------------------
    private final Map<String, Account> accounts = new ConcurrentHashMap<>();
    private final Map<String, Product> products = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Row-level locks — maps rowId -> ownerThreadName
    // -------------------------------------------------------------------------
    private final Map<String, String> rowLocks = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // MVCC: global transaction timestamp counter
    // -------------------------------------------------------------------------
    private final AtomicLong txClock = new AtomicLong(0);

    /**
     * A committed version entry: the account value + the commit timestamp.
     * Real MVCC engines (PostgreSQL, InnoDB) store this in the undo log / version chain.
     */
    public record AccountVersion(Account account, long commitTs) {}

    /** Version chain per account: all committed versions, oldest first. */
    private final Map<String, List<AccountVersion>> versionChain = new ConcurrentHashMap<>();

    /**
     * Uncommitted (dirty) writes: written by a TX but not yet committed.
     * Used by DirtyReadDemo to let READ_UNCOMMITTED transactions see them.
     */
    private final Map<String, Account> uncommittedWrites = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Table-level lock flag
    // -------------------------------------------------------------------------
    private volatile boolean tableLocked = false;
    private volatile String  tableLockOwner = null;

    /** Simulated DB query latency in milliseconds. */
    private final long latencyMs;

    public InMemoryDatabase(long latencyMs) {
        this.latencyMs = latencyMs;
        seed();
    }

    private void seed() {
        accounts.put("acc-1", new Account("acc-1", "Alice",   1_000.00));
        accounts.put("acc-2", new Account("acc-2", "Bob",     2_500.00));
        accounts.put("acc-3", new Account("acc-3", "Charlie",   500.00));

        products.put("p-1", new Product("p-1", "Laptop",  10));
        products.put("p-2", new Product("p-2", "Phone",   25));
        products.put("p-3", new Product("p-3", "Headset", 50));
    }

    // -------------------------------------------------------------------------
    // Account CRUD
    // -------------------------------------------------------------------------

    /** Returns a COPY of the row (what a real SELECT returns — a snapshot). */
    public Optional<Account> findAccount(String id) {
        simulateLatency();
        return Optional.ofNullable(accounts.get(id)).map(Account::copy);
    }

    /**
     * Unconditional save — used internally after lock guards have been applied.
     * Simulates a raw UPDATE without version check.
     */
    public void saveAccount(Account account) {
        simulateLatency();
        accounts.put(account.getId(), account);
        System.out.printf("[DB] Account saved: %s%n", account);
    }

    /**
     * Optimistic update — only succeeds if the stored version matches.
     * Simulates: UPDATE accounts SET balance=?, version=version+1
     *            WHERE id=? AND version=?
     *
     * @return true if updated, false if version mismatch (stale read)
     */
    public synchronized boolean updateAccountOptimistic(Account updated) {
        simulateLatency();
        Account stored = accounts.get(updated.getId());
        if (stored == null) return false;
        if (stored.getVersion() != updated.getVersion()) {
            System.out.printf("[DB] Optimistic conflict on acc='%s': stored version=%d, caller version=%d%n",
                    updated.getId(), stored.getVersion(), updated.getVersion());
            return false;
        }
        updated.incrementVersion();
        accounts.put(updated.getId(), updated);
        System.out.printf("[DB] Optimistic update OK: %s%n", updated);
        return true;
    }

    // -------------------------------------------------------------------------
    // Product CRUD
    // -------------------------------------------------------------------------

    public Optional<Product> findProduct(String id) {
        simulateLatency();
        return Optional.ofNullable(products.get(id)).map(Product::copy);
    }

    public void saveProduct(Product product) {
        simulateLatency();
        products.put(product.getId(), product);
        System.out.printf("[DB] Product saved: %s%n", product);
    }

    public synchronized boolean updateProductOptimistic(Product updated) {
        simulateLatency();
        Product stored = products.get(updated.getId());
        if (stored == null) return false;
        if (stored.getVersion() != updated.getVersion()) {
            System.out.printf("[DB] Optimistic conflict on product='%s': stored v=%d, caller v=%d%n",
                    updated.getId(), stored.getVersion(), updated.getVersion());
            return false;
        }
        updated.incrementVersion();
        products.put(updated.getId(), updated);
        System.out.printf("[DB] Optimistic update OK: %s%n", updated);
        return true;
    }

    // -------------------------------------------------------------------------
    // Row-level lock management (used by Pessimistic locking demos)
    // -------------------------------------------------------------------------

    /**
     * Acquires an exclusive row lock for the given rowId.
     * Blocks (busy-waits with sleep) until the lock is available.
     *
     * In a real DB engine this is handled transparently inside the storage
     * engine (e.g. InnoDB row-level latch). Here we simulate it explicitly
     * so the behaviour is visible in console output.
     */
    public void lockRow(String rowId) {
        String owner = Thread.currentThread().getName();
        while (true) {
            String existing = rowLocks.putIfAbsent(rowId, owner);
            if (existing == null) {
                System.out.printf("[DB] Row lock ACQUIRED: row='%s' by thread='%s'%n", rowId, owner);
                return;
            }
            if (existing.equals(owner)) {
                return; // re-entrant: same thread already owns the lock
            }
            System.out.printf("[DB] Row lock WAITING: row='%s' — thread='%s' blocked by '%s'%n",
                    rowId, owner, existing);
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    /** Releases the row lock. Only the owning thread may release. */
    public void unlockRow(String rowId) {
        String owner = Thread.currentThread().getName();
        boolean removed = rowLocks.remove(rowId, owner);
        if (removed) {
            System.out.printf("[DB] Row lock RELEASED: row='%s' by thread='%s'%n", rowId, owner);
        }
    }

    /** Returns all currently held row lock entries (for diagnostics). */
    public Set<Map.Entry<String, String>> getRowLocks() {
        return rowLocks.entrySet();
    }

    // -------------------------------------------------------------------------
    // Table-level lock (used by Table-Lock demo)
    // -------------------------------------------------------------------------

    public synchronized void lockTable() {
        String owner = Thread.currentThread().getName();
        while (tableLocked) {
            System.out.printf("[DB] Table lock WAITING: thread='%s' blocked by '%s'%n", owner, tableLockOwner);
            try { wait(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        tableLocked    = true;
        tableLockOwner = owner;
        System.out.printf("[DB] Table lock ACQUIRED by thread='%s'%n", owner);
    }

    public synchronized void unlockTable() {
        tableLocked    = false;
        tableLockOwner = null;
        System.out.printf("[DB] Table lock RELEASED by thread='%s'%n", Thread.currentThread().getName());
        notifyAll();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void simulateLatency() {
        if (latencyMs <= 0) return;
        try { Thread.sleep(latencyMs); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public void reset() {
        accounts.clear();
        products.clear();
        rowLocks.clear();
        versionChain.clear();
        uncommittedWrites.clear();
        tableLocked    = false;
        tableLockOwner = null;
        seed();
        // Seed initial version chain snapshots
        accounts.forEach((id, acc) ->
            versionChain.computeIfAbsent(id, k -> new ArrayList<>())
                        .add(new AccountVersion(acc.copy(), 0)));
    }

    // -------------------------------------------------------------------------
    // MVCC — Transaction timestamps
    // -------------------------------------------------------------------------

    /** Begin a new transaction: returns a start timestamp (snapshot point). */
    public long beginTx() {
        long ts = txClock.incrementAndGet();
        System.out.printf("[MVCC] TX started with snapshot timestamp=%d (thread='%s')%n",
                ts, Thread.currentThread().getName());
        return ts;
    }

    // -------------------------------------------------------------------------
    // MVCC — Snapshot read (REPEATABLE READ / SERIALIZABLE)
    // -------------------------------------------------------------------------

    /**
     * Returns the latest committed version of the account that was committed
     * AT OR BEFORE the given snapshot timestamp.
     *
     * This is exactly how PostgreSQL MVCC works:
     *   - Each TX gets a snapshot at BEGIN time.
     *   - Reads see only versions committed before the snapshot.
     *   - Versions committed AFTER the snapshot are invisible — they don't exist yet
     *     from the perspective of this transaction.
     */
    public Optional<Account> snapshotRead(String id, long snapshotTs) {
        simulateLatency();
        List<AccountVersion> chain = versionChain.get(id);
        if (chain == null) return Optional.empty();

        // Walk the version chain newest-first; return the latest visible version
        AccountVersion visible = null;
        for (AccountVersion v : chain) {
            if (v.commitTs() <= snapshotTs) {
                visible = v;
            }
        }
        if (visible != null) {
            System.out.printf("[MVCC] Snapshot read id='%s' at snapshotTs=%d -> version committed at ts=%d: %s%n",
                    id, snapshotTs, visible.commitTs(), visible.account());
            return Optional.of(visible.account().copy());
        }
        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // MVCC — Committed write (publishes a new version to the chain)
    // -------------------------------------------------------------------------

    /**
     * Commits a new version of the account to the version chain.
     * The commit timestamp becomes visible to all future snapshot reads
     * whose snapshotTs >= commitTs.
     */
    public long commitVersion(Account account) {
        long commitTs = txClock.incrementAndGet();
        versionChain.computeIfAbsent(account.getId(), k -> new ArrayList<>())
                    .add(new AccountVersion(account.copy(), commitTs));
        accounts.put(account.getId(), account); // also update current row
        System.out.printf("[MVCC] Version committed: id='%s' commitTs=%d -> %s%n",
                account.getId(), commitTs, account);
        return commitTs;
    }

    // -------------------------------------------------------------------------
    // Dirty read support — uncommitted write log
    // -------------------------------------------------------------------------

    /**
     * Writes to the uncommitted buffer WITHOUT publishing to the version chain.
     * A READ_UNCOMMITTED reader can see this; a READ_COMMITTED reader cannot.
     */
    public void writeUncommitted(Account account) {
        uncommittedWrites.put(account.getId(), account.copy());
        System.out.printf("[DIRTY] Uncommitted write: id='%s' -> %s (NOT yet committed)%n",
                account.getId(), account);
    }

    /**
     * Simulates ROLLBACK — discards the uncommitted write so dirty readers
     * that acted on it were working with data that "never existed".
     */
    public void rollbackUncommitted(String id) {
        Account discarded = uncommittedWrites.remove(id);
        System.out.printf("[DIRTY] ROLLBACK: uncommitted write for id='%s' discarded (%s)%n",
                id, discarded);
    }

    /**
     * READ_UNCOMMITTED: returns the dirty (uncommitted) value if present,
     * otherwise falls back to the last committed value.
     */
    public Optional<Account> readUncommitted(String id) {
        simulateLatency();
        Account dirty = uncommittedWrites.get(id);
        if (dirty != null) {
            System.out.printf("[DIRTY READ] id='%s' -> reading UNCOMMITTED value: %s%n", id, dirty);
            return Optional.of(dirty.copy());
        }
        return Optional.ofNullable(accounts.get(id)).map(Account::copy);
    }

    // -------------------------------------------------------------------------
    // Range query support (phantom read demo)
    // -------------------------------------------------------------------------

    /**
     * Returns all accounts whose balance is within [minBalance, maxBalance].
     * Used by PhantomReadDemo: the result set can change between two calls
     * if another TX inserts/deletes accounts in that range.
     */
    public List<Account> rangeQuery(double minBalance, double maxBalance) {
        simulateLatency();
        List<Account> result = new ArrayList<>();
        for (Account acc : accounts.values()) {
            if (acc.getBalance() >= minBalance && acc.getBalance() <= maxBalance) {
                result.add(acc.copy());
            }
        }
        System.out.printf("[DB] Range query [%.0f - %.0f] -> %d rows%n",
                minBalance, maxBalance, result.size());
        return result;
    }

    /** Insert a brand-new account (used by phantom read demo). */
    public void insertAccount(Account account) {
        simulateLatency();
        accounts.put(account.getId(), account);
        versionChain.computeIfAbsent(account.getId(), k -> new ArrayList<>())
                    .add(new AccountVersion(account.copy(), txClock.incrementAndGet()));
        System.out.printf("[DB] INSERT account: %s%n", account);
    }

    /** Delete an account (used by phantom read demo). */
    public void deleteAccount(String id) {
        simulateLatency();
        accounts.remove(id);
        System.out.printf("[DB] DELETE account id='%s'%n", id);
    }
}
