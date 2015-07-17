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

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.exceptions.ConnectionException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * This class represents a elasticsearchs connetions handler.
 * Created by jmgomez on 28/08/14.
 */
public class ElasticSearchConnectionHandler extends ConnectionHandler {

    /**
     * Constructor.
     *
     * @param configuration the configuration.
     */
    public ElasticSearchConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    /**
     * This method creates a elasticsearch connection.
     *
     * @param iCredentials           the credentials to connect with the database.
     * @param connectorClusterConfig the cluster configuration.
     * @throws ConnectionException if the connection is not established.
     * @return a elasticsearch connection.
     */
    @Override
    protected Connection createNativeConnection(ICredentials iCredentials,
            ConnectorClusterConfig connectorClusterConfig) throws ConnectionException {
        Connection connection;
        if (isNodeClient(connectorClusterConfig)) {
            connection = new NodeConnection(iCredentials, connectorClusterConfig);
        }
        else {
            try {connection = new TransportConnection(iCredentials, connectorClusterConfig);}
            catch (ExecutionException exception){
                throw new ConnectionException("The connection could not be established", exception);
            }
        }
        if (!connection.isConnected()) {
             throw new ConnectionException("The connection could not be established");}
        return connection;
    }

    /**
     * Return true if the config says that the connection is nodeClient. false in other case.
     *
     * @param config the configuration.
     * @return true if is configure to be a node connection. False in other case.
     */
    private boolean isNodeClient(ConnectorClusterConfig config) {
        return Boolean.parseBoolean(config.getConnectorOptions().get(ConfigurationOptions.NODE_TYPE.getManifestOption()));
    }

}
