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

import com.stratio.connector.elasticsearch.core.engine.utils.Constants;

/**
 * This enum has the options for Elasticsearch.
 * Created by jmgomez on 1/09/14.
 */
public enum ConfigurationOptions {

    /**
     * The elasticserach node type property.
     */
    NODE_TYPE("node_type", "node_type", Constants.FALSE),
    /**
     * The elasticserach node data property.
     */
    NODE_DATA("node.data", "node.data",Constants.FALSE),
    /**
     * The elasticserach node master property.
     */
    NODE_MASTER("node.master", "node.master",Constants.FALSE),
    /**
     * The elasticserach transfer sniff type property.
     */
    TRANSPORT_SNIFF("client.transport.sniff", "client.transport.sniff",Constants.TRUE),
    /**
     * The elastichseach cluser name.
     */
    CLUSTER_NAME("Cluster Name","cluster.name","", "clusterName"),
    /**
     * The hosts ip.
     */
    HOST("Hosts","Hosts", new String[] { "localhost" }),
    /**
     * The hosts ports.
     */
    PORT("Native Ports", "Native Ports",new String[] { "c" }),
    /**
     * The elasticsearch coerce property.
     */
    COERCE("index.mapping.coerce","index.mapping.coerce", Constants.FALSE),
    /**
     * The elasticsearch mapper dynamic property.
     */
    DYNAMIC("index.mapper.dynamic", "index.mapper.dynamic",Constants.FALSE);

    /**
     * The name of the option in crossdata manifest.
     */
    private final String manifestOption;
    /**
     * The default value of the options.
     */
    private final String[] defaultValue;
    /**
     * The option name in elasticsearch.
     */
    private final String elasticSearchOption;

    /**
     * Constructor.
     *
     * @param manifestOption   the name of the option.
     * @param defaultValue the default value of the option.
     */
    ConfigurationOptions(String manifestOption, String elasticSearchOption, String... defaultValue) {
        this.manifestOption = manifestOption;
        this.defaultValue = defaultValue;
        this.elasticSearchOption = elasticSearchOption;

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
     * Return the option name defined in the manifest.
     *
     * @return the option name.
     */
    public String getManifestOption() {
        return manifestOption;
    }



    /**
     * Return the option name to ElasticSearch.
     *
     * @return the option name.
     */
    public String getElasticSearchOption() {
        return elasticSearchOption;
    }

}
