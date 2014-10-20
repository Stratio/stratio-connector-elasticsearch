/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.elasticsearch.core.engine;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
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
public class ElasticsearchMetadataEngine extends CommonsMetadataEngine<Client> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ContentBuilderCreator deepContentBuilder = new ContentBuilderCreator();

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
     * @param indexMetaData the index configuration.
     * @throws UnsupportedException if any operation is not supported.
     * @throws ExecutionException   if an error occur.
     */

    @Override
    protected void createCatalog(CatalogMetadata indexMetaData, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        try {

            createESIndex(indexMetaData, connection);

        } catch (HandlerConnectionException e) {
            throwHandlerConnectionException(e, "createCatalog");
        }
    }

    /**
     * This method create a type in ES.
     *
     * @param typeMetadata the type configuration.
     * @throws UnsupportedException if any operation is not supported.
     * @throws ExecutionException   if an error occur.
     */
    @Override
    protected void createTable(TableMetadata typeMetadata, Connection<Client> connection)
            throws UnsupportedException,
            ExecutionException {
        try {

            String indexName = typeMetadata.getName().getCatalogName().getName();
            XContentBuilder xContentBuilder = deepContentBuilder.createTypeSource(typeMetadata);

            recoveredClient(connection).admin().indices().preparePutMapping().setIndices(indexName)
                    .setType(typeMetadata.getName().getName()).setSource(xContentBuilder).execute()
                    .actionGet();
        } catch (HandlerConnectionException | ElasticsearchException e) {
            e.printStackTrace();
            throwHandlerConnectionException(e, "createTable");
        }
    }

    /**
     * This method drop a index in ES.
     *
     * @param indexName the index name.
     */

    @Override
    protected void dropCatalog(CatalogName indexName, Connection<Client> connection)
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
     * @param typeName the type name.
     */
    @Override
    protected void dropTable(TableName typeName, Connection<Client> connection)
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
    protected void createIndex(IndexMetadata indexMetadata, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    private void throwHandlerConnectionException(Exception e, String operation)
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

    private void createESIndex(CatalogMetadata indexMetaData, Connection connection)
            throws HandlerConnectionException, ExecutionException {

        CreateIndexRequestBuilder createIndexRequestBuilder = recoveredClient(connection).admin().indices()
                .prepareCreate(indexMetaData.getName().getName());
        createIndexRequestBuilder.setSettings(transformOptions(indexMetaData));

        createIndexRequestBuilder.execute().actionGet();

    }

    private Map<String, String> transformOptions(CatalogMetadata indexMetaData) throws ExecutionException {

        Map<String, String> transformOptions = new HashMap<>();
        Map<Selector, Selector> options = indexMetaData.getOptions();
        if (options != null) {
            for (Selector key : options.keySet()) {
                transformOptions
                        .put(SelectorHelper.getValue(String.class, key),
                                SelectorHelper.getValue(String.class, options.get(key)));
            }
        }
        return transformOptions;
    }

}

