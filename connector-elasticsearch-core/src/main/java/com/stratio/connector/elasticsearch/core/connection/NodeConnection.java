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

package com.stratio.connector.elasticsearch.core.connection;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;

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
        connectionName  = config.getName().getName();
        logger.info("Elasticsearch Node connection established ");

    }

    public void close() {
        if (node != null) {
            node.close();
            isConnect = false;
            node = null;
            elasticClient = null;
            logger.info("ElasticSearch  connection ["+connectionName+"] close");
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
