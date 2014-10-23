/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.configuration;

/**
 * This enum has the options for Elasticsearch.
 * Created by jmgomez on 1/09/14.
 */
public enum ConfigurationOptions {

    /**
     * The elasticserach node type property.
     */
    NODE_TYPE("node_type", "false"),
    /**
     * The elasticserach node data property.
     */
    NODE_DATA("node.data", "false"),
    /**
     * The elasticserach node master property.
     */
    NODE_MASTER("node.master", "false"),
    /**
     * The elasticserach transfer sniff type property.
     */
    TRANSPORT_SNIFF("client.transport.sniff", "true"),
    /**
     * The elastichseach cluser name.
     */
    CLUSTER_NAME("cluster.name", "cluster_name"),
    /**
     * The hosts ip.
     */
    HOST("Hosts", new String[] { "localhost" }),
    /**
     * The hosts ports.
     */
    PORT("Port", new String[] { "9300" }),
    /**
     * The elasticsearch coerce property.
     */
    COERCE("index.mapping.coerce", "false"),
    /**
     * The elasticsearch mapper dynamic property.
     */
    DYNAMIC("index.mapper.dynamic", "false");

    /**
     * The name of the option.
     */
    private final String optionName;
    /**
     * The default value of the options.
     */
    private final String[] defaultValue;

    /**
     * Constructor.
     *
     * @param optionName   the name of the option.
     * @param defaultValue the default value of the option.
     */
    ConfigurationOptions(String optionName, String... defaultValue) {
        this.optionName = optionName;
        this.defaultValue = defaultValue;

    }

    /**
     * return the default value.
     *
     * @return the default value.
     */
    public String[] getDefaultValue() {
        return defaultValue.clone();
    }

    /**
     * Return the option name.
     *
     * @return the option name.
     */
    public String getOptionName() {
        return optionName;
    }

}
