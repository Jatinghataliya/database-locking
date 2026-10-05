package com.example.locking.model;

/**
 * Represents a product inventory row in the simulated database.
 * Used by stock-deduction demos to illustrate concurrent write races.
 */
public class Product {
    private final String id;
    private String name;
    private int stock;
    private int version;

    public Product(String id, String name, int stock) {
        this.id      = id;
        this.name    = name;
        this.stock   = stock;
        this.version = 0;
    }

    public Product copy() {
        Product p = new Product(id, name, stock);
        p.version = this.version;
        return p;
    }

    public String getId()      { return id; }
    public String getName()    { return name; }
    public int    getStock()   { return stock; }
    public int    getVersion() { return version; }

    public void setStock(int stock)   { this.stock   = stock; }
    public void setName(String name)  { this.name    = name; }
    public void incrementVersion()    { this.version++; }

    @Override
    public String toString() {
        return String.format("Product{id='%s', name='%s', stock=%d, version=%d}",
                id, name, stock, version);
    }
}
