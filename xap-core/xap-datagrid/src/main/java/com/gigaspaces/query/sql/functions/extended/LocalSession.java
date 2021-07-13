package com.gigaspaces.query.sql.functions.extended;

public class LocalSession {
    private String username;

    public LocalSession() {
    }

    public LocalSession(String username) {
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
