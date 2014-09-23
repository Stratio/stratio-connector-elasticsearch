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
package com.stratio.connector.elasticsearch.core.engine;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

/**
 * This class is the responsible of manage the ElasticSearchMetadata
 *
 * @author darroyo
 */
public class ElasticsearchMetadataEngine extends CommonsMetadataEngine {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    ContentBuilderCreator deepContentBuilder = new ContentBuilderCreator();

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handle.
     */
    public ElasticsearchMetadataEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    /**
     * This method create a index in ES.
     *
     * @param targetCluster the cluster to be created.
     * @param indexMetaData the index configuration.
     * @throws UnsupportedException if any operation is not supported.
     * @throws ExecutionException   if an error occur.
     */

    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata indexMetaData, Connection connection)
            throws UnsupportedException, ExecutionException {
        try {

            createESIndex(indexMetaData, connection);
            addAllTypesInTheCatalog(targetCluster, indexMetaData.getTables());
        } catch (HandlerConnectionException e) {
            throwHandlerConnectionException(e, "createCatalog");
        }
    }

    /**
     * This method create a type in ES.
     *
     * @param targetCluster the cluster to be created.
     * @param typeMetadata  the type configuration.
     * @throws UnsupportedException if any operation is not supported.
     * @throws ExecutionException   if an error occur.
     */
    @Override
    public void createTable(ClusterName targetCluster, TableMetadata typeMetadata, Connection connection)
            throws UnsupportedException,
            ExecutionException {
        try {

            String indexName = typeMetadata.getName().getCatalogName().getName();
            XContentBuilder xContentBuilder = deepContentBuilder.createTypeSource(typeMetadata);

            recoveredClient(connection).admin().indices().preparePutMapping().setIndices(indexName)
                    .setType(typeMetadata.getName().getName()).setSource(xContentBuilder).execute().actionGet();
        } catch (HandlerConnectionException e) {
            throwHandlerConnectionException(e, "createTable");
        }
    }

    /**
     * This method drop a index in ES.
     *
     * @param targetCluster the cluster to be created.
     * @param indexName     the index name.
     */

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName indexName, Connection connection)
            throws ExecutionException {
        DeleteIndexResponse delete = null;
        try {
            delete = recoveredClient(connection).admin().indices().delete(new DeleteIndexRequest(indexName.getName()))
                    .actionGet();
            if (!delete.isAcknowledged()) {
                throw new ExecutionException("dropCatalog request has not been acknowledged");
            }
        } catch (HandlerConnectionException e) {
            throwHandlerConnectionException(e, "dropCatalog");
        }

    }

    /**
     * This method drop a type in ES.
     *
     * @param targetCluster the cluster to be created.
     * @param typeName      the type name.
     */
    @Override
    public void dropTable(ClusterName targetCluster, TableName typeName, Connection connection)
            throws ExecutionException {
        DeleteMappingResponse delete = null;
        try {
            delete = recoveredClient(connection).admin().indices()
                    .prepareDeleteMapping(typeName.getCatalogName().getName()).setType(typeName.getName()).execute()
                    .actionGet();
            if (!delete.isAcknowledged()) {
                throw new ExecutionException("dropTable request has not been acknowledged");
            }
        } catch (HandlerConnectionException e) {
            throwHandlerConnectionException(e, "dropTable");

        }
    }

    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexMetadata, Connection connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    private void throwHandlerConnectionException(HandlerConnectionException e, String operation)
            throws ExecutionException {
        String msg = "Error find ElasticSearch client in " + operation + ". " + e.getMessage();
        logger.error(msg);
        throw new ExecutionException(msg, e);
    }

    /**
     * This method return the concrete ES Client for a cluster,
     *
     * @param connection the cluster identification.
     * @return the ES Client.
     */
    private Client recoveredClient(Connection connection) throws HandlerConnectionException {
        return (Client) connection.getNativeConnection();
    }

    private void createESIndex(CatalogMetadata indexMetaData, Connection connection) throws HandlerConnectionException {
        CreateIndexRequestBuilder createIndexRequestBuilder = recoveredClient(connection).admin().indices()
                .prepareCreate(indexMetaData.getName().getName());
        createIndexRequestBuilder.setSettings(transformOptions(indexMetaData));

        createIndexRequestBuilder.execute().actionGet();

    }

    private Map<String, String> transformOptions(CatalogMetadata indexMetaData) {
        SelectorHelper selectorHelper = new SelectorHelper();
        Map<String, String> transformOptions = new HashMap<>();
        Map<Selector, Selector> options = indexMetaData.getOptions();
        for (Selector key : options.keySet()) {
            transformOptions
                    .put(selectorHelper.getStringFieldValue(key), selectorHelper.getStringFieldValue(options.get(key)));
        }
        return transformOptions;
    }

    private void addAllTypesInTheCatalog(ClusterName targetCluster, Map<TableName, TableMetadata> types)
            throws UnsupportedException, ExecutionException {
        if (types != null) {
            for (TableName tableName : types.keySet()) {
                createTable(targetCluster, types.get(tableName));
            }
        }
    }
}

