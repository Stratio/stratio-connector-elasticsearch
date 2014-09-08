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


import com.stratio.connector.connection.Connection;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * This class represents a logic connection.
 * Created by jmgomez on 28/08/14.
 */
public class NodeConnection implements Connection<Client> {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());


    /**
     * The Elasticsearch client.
     */
    private Client elasticClient = null;  //REVIEW posiblemente esta clase desaparezca ya que la conexion no esta aqui.

    private Node node = null;
    private boolean isConnect = false;


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
        logger.info("Elasticsearch Node connection established ");

    }

    public void close() {
        if (node != null) {
            node.close();
            isConnect = false;
            node = null;
            elasticClient = null;

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
