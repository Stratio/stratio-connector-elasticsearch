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

import com.stratio.connector.commons.TimerJ;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
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
public class TransportConnection extends Connection<Client> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The Elasticsearch client.
     */
    private Client elasticClient = null;


    /**
     * Constructor.
     *
     * @param credentiasl the credentials.
     * @param config      The cluster configuration.
     */
    public TransportConnection(ICredentials credentiasl, ConnectorClusterConfig config)  throws ExecutionException {

        elasticClient = new TransportClient(ElasticsearchClientConfiguration.getSettings(config))
                .addTransportAddresses(ElasticsearchClientConfiguration.getTransportAddress(config));
        logger.info("Elasticsearch Transport connection established ");


    }

    /**
     * Close the connection.
     */
    @TimerJ
    public void close() {
        if (elasticClient != null) {
            elasticClient.close();

            elasticClient = null;

        }

    }

    /**
     * Return the connection status.
     *
     * @return true if the connection is open. False in other case.
     */
    @Override
    @TimerJ
    public boolean isConnected() {

        return (elasticClient!=null && !((TransportClient)elasticClient).connectedNodes().isEmpty());


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
