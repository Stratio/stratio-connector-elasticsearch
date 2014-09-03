package com.stratio.connector.commons.util;

/**
 * Created by jmgomez on 3/09/14.
 */
public class Parser {


    public String[] hosts(String hosts) {
        return hosts.split(",");

    }

    public String[] ports(String ips) {
        return ips.split(",");
    }
}
