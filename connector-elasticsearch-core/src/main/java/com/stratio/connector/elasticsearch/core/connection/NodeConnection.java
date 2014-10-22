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

    private Node node = null;
    private boolean isConnect = false;

    private String connectionName;

    /**
     * Constructor.
     *
     * @param credentiasl the credentials.
     * @param config      The cluster configuration.
     */
    public NodeConnection(ICredentials credentiasl, ConnectorClusterConfig config) {
        NodeBuilder nodeBuilder = nodeBuilder();

        node = nodeBuilder.settings(ElasticsearchClientConfiguration.getSettings(config)).node();
        elasticClient = node.client();
        isConnect = true;
        connectionName = config.getName().getName();
        logger.info("Elasticsearch Node connection established ");

    }

    public void close() {
        if (node != null) {
            node.close();
            isConnect = false;
            node = null;
            elasticClient = null;
            logger.info("ElasticSearch  connection [" + connectionName + "] close");
        }

    }

    @Override
    public boolean isConnect() {
        return isConnect;
    }

    @Override
    public Client getNativeConnection() {
        return elasticClient;
    }

}
