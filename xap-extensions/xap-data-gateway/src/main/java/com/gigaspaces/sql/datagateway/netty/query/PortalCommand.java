package com.gigaspaces.sql.datagateway.netty.query;

enum PortalCommand {
    SHOW("SHOW"),
    SELECT("SELECT"),
    SET("SET");

    private final String tag;

    PortalCommand(String tag) {
        this.tag = tag;
    }

    public String tag() {
        return tag;
    }
}
