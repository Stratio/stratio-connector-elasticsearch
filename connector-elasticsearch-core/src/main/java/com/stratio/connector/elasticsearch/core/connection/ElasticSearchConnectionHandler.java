/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.connection;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.elasticsearch.core.configuration.ConfigurationOptions;
import com.stratio.crossdata.common.connector.ConnectorClusterConfig;
import com.stratio.crossdata.common.connector.IConfiguration;
import com.stratio.crossdata.common.security.ICredentials;

/**
 * Created by jmgomez on 28/08/14.
 */
public class ElasticSearchConnectionHandler extends ConnectionHandler {

    public ElasticSearchConnectionHandler(IConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected Connection createNativeConnection(ICredentials iCredentials,
            ConnectorClusterConfig connectorClusterConfig) {
        Connection connection;
        if (isNodeClient(connectorClusterConfig)) {
            connection = new NodeConnection(iCredentials, connectorClusterConfig);
        } else {
            connection = new TransportConnection(iCredentials, connectorClusterConfig);
        }
        return connection;
    }

    private boolean isNodeClient(ConnectorClusterConfig config) {
        return Boolean.parseBoolean((String) config.getOptions().get(ConfigurationOptions.NODE_TYPE.getOptionName()));
    }

}
