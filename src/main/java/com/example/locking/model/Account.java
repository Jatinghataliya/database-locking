package com.example.locking.model;

/**
 * Represents a bank account row in the simulated database.
 *
 * The 'version' field is the key ingredient for Optimistic Locking:
 * every successful UPDATE increments it. A stale reader whose version
 * no longer matches the DB is rejected — no DB lock ever held.
 */
public class Account {
    private final String id;
    private String owner;
    private double balance;
    private int version;

    public Account(String id, String owner, double balance) {
        this.id      = id;
        this.owner   = owner;
        this.balance = balance;
        this.version = 0;
    }

    /** Returns a deep copy — simulates what a DB SELECT returns (a snapshot in time). */
    public Account copy() {
        Account a = new Account(id, owner, balance);
        a.version = this.version;
        return a;
    }

    public String getId()      { return id; }
    public String getOwner()   { return owner; }
    public double getBalance() { return balance; }
    public int    getVersion() { return version; }

    public void setBalance(double balance)  { this.balance = balance; }
    public void setOwner(String owner)      { this.owner   = owner; }
    public void incrementVersion()          { this.version++; }

    @Override
    public String toString() {
        return String.format("Account{id='%s', owner='%s', balance=%.2f, version=%d}",
                id, owner, balance, version);
    }
}
