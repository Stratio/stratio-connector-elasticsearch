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


import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.configuration.ElasticsearchClientConfiguration;
import com.stratio.meta.common.connector.ConnectorClusterConfig;
import com.stratio.meta.common.security.ICredentials;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a logic connection.
 * Created by jmgomez on 28/08/14.
 */
public class TransportConnection implements Connection<Client> {


    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    ElasticsearchClientConfiguration elasticsearchClientConfiguration = new ElasticsearchClientConfiguration();

    /**
     * The Elasticsearch client.
     */
    private Client elasticClient = null;  //REVIEW posiblemente esta clase desaparezca ya que la conexion no esta aqui.
    private boolean isConnect = false;


    /**
     * Constructor.
     *
     * @param credentiasl the credentials.
     * @param config      The cluster configuration.
     */
    public TransportConnection(ICredentials credentiasl, ConnectorClusterConfig config) {


        elasticClient = new TransportClient(ElasticsearchClientConfiguration.getSettings(config))
                .addTransportAddresses(elasticsearchClientConfiguration.getTransporAddress(config));
        logger.info("Elasticsearch Transport connection established ");


        isConnect = true;
    }


    public void close() {
        if (elasticClient != null) {
            elasticClient.close();
            isConnect = false;
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
