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

package com.stratio.connector.elasticsearch.core.connection;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import com.stratio.connector.commons.TimerJ;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * This class represents a logic connection.
 * Created by jmgomez on 28/08/14.
 */
public class NodeConnection extends Connection<Client> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The Elasticsearch client.
     */
    private Client elasticClient = null;

    /**
     * The elasticsearch node connection.
     */
    private Node node = null;


    /**
     * Store the connection name.
     */
    private String connectionName;

    /**
     * Constructor.
     *
     * @param credentials the credentials.
     * @param config      The cluster configuration.
     */
    public NodeConnection(ICredentials credentials, ConnectorClusterConfig config) {
        NodeBuilder nodeBuilder = nodeBuilder();

        node = nodeBuilder.settings(ElasticsearchClientConfiguration.getSettings(config)).node();
        elasticClient = node.client();

        connectionName = config.getName().getName();
        logger.info("Elasticsearch Node connection established ");

    }

    /**
     * Close the connection.
     */
    @TimerJ
    public void close() {
        if (node != null) {
            node.close();
            node = null;
            elasticClient = null;
            logger.info("ElasticSearch  connection [" + connectionName + "] close");
        }

    }

    /**
     * Retun the connection status.
     *
     * @return true if the connection is open. False in other case.
     */
    @Override
    @TimerJ
    public boolean isConnected() {
        return (node!=null && !node.isClosed());
    }

    /**
     * Return the native connection.
     *
     * @return the native connection.
     */
    @Override
    @TimerJ
    public Client getNativeConnection() {
        return elasticClient;
    }

}
