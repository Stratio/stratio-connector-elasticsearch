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

package com.stratio.connector.elasticsearch.core;


import com.stratio.connector.elasticsearch.core.configuration.ConnectionConfigurationCreator;
import com.stratio.connector.elasticsearch.core.configuration.SupportedOperationsCreator;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandle;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchMetadataEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.meta.common.connector.*;
import com.stratio.meta.common.security.ICredentials;
import com.stratio.meta2.common.data.ClusterName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;


/**
 * This class implements the connector for Elasticsearch.
 */
public class ElasticsearchConnector implements IConnector {


    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connectionHandle.
     */
    private ElasticSearchConnectionHandle connectionHandle = null;


    /**
     * Create a connection to Elasticsearch.
     * The client will be a transportClient by default unless stratio nodeClient is specified.
     *
     * @param configuration the connection configuration. It must be not null.
     *                      onnection.
     */

    @Override
    public void init(IConfiguration configuration) {

        connectionHandle = new ElasticSearchConnectionHandle(configuration);

    }


    /**
     * Create a connection with ElasticSearch.
     *
     * @param credentials the credentials.
     * @param config      the connection configuration.
     */
    @Override
    public void connect(ICredentials credentials, ConnectorClusterConfig config) {
        connectionHandle.createConnection(credentials, config);
    }


    /**
     * It close the  connection to ElasticSearch.
     *
     * @param name the connection identifier.
     */
    @Override
    public void close(ClusterName name) {
        connectionHandle.closeConnection(name.getName());

    }


    @Override
    public String getConnectorName() {
        return "ElasticSearch";
    }

    /**
     * Return the DataStore Name.
     *
     * @return DataStore Name
     */
    @Override
    public String[] getDatastoreName() {
        return new String[]{"ElasticSearch"};
    }

    /**
     * Return the StorageEngine.
     *
     * @return the StorageEngine
     */
    @Override
    public IStorageEngine getStorageEngine() {

        return new ElasticsearchStorageEngine(connectionHandle);

    }


    /**
     * Return the QueryEngine.
     *
     * @return the QueryEngine
     */
    @Override
    public IQueryEngine getQueryEngine() {

        return new ElasticsearchQueryEngine(connectionHandle);
    }

    /**
     * Return the MetadataEngine.
     *
     * @return the MetadataEngine
     */
    @Override
    public IMetadataEngine getMetadataEngine() {
        return new ElasticsearchMetadataEngine(connectionHandle);
    }


    /**
     * Return the supported operations
     *
     * @return the supported operations.
     */
    public Map<Operations, Boolean> getSupportededOperations() {
        return SupportedOperationsCreator.getSupportedOperations();
    }


    /**
     * Return the supported configuration options
     *
     * @return the the supported configuration options.
     */
    public Set<ConnectionConfiguration> getConnectionConfiguration() {
        return ConnectionConfigurationCreator.getConfiguration();
    }


    /**
     * The connection status.
     *
     * @param name the cluster Name.
     * @return true if the driver's client is not null.
     */
    @Override
    public boolean isConnected(ClusterName name) {

        return connectionHandle.isConnected(name.getName());

    }


}
