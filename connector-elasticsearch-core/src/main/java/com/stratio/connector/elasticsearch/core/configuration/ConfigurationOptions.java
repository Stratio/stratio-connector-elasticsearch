/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.elasticsearch.core.configuration;

/**
 * Created by jmgomez on 1/09/14.
 */
public enum ConfigurationOptions {

    NODE_TYPE("node_type", "false"),
    NODE_DATA("node.data", "false"),
    NODE_MASTER("node.master", "false"),
    TRANSPORT_SNIFF("client.transport.sniff", "true"),
    CLUSTER_NAME("cluster.name", "cluster_name"),
    HOST("Hosts", new String[] { "localhost" }),
    PORT("Port", new String[] { "9300" }),
    COERCE("index.mapping.coerce","false"),
    DYNAMIC("index.mapper.dynamic","false")
    ;


    private final String optionName;
    private final String[] defaultValue;

    ConfigurationOptions(String optionName, String... defaultValue) {
        this.optionName = optionName;
        this.defaultValue = defaultValue;

    }

    public String[] getDefaultValue() {
        return defaultValue;
    }

    public String getOptionName() {
        return optionName;
    }

}
