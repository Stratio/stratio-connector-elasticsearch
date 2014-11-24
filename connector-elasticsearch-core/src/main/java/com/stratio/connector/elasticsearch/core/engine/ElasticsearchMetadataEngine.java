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
import com.stratio.connector.elasticsearch.core.engine.metadata.AlterTableFactory;
import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * This class is the responsible of manage the ElasticSearchMetadata.
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

    @Override protected void alterTable(TableName name, AlterOptions alterOptions, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {

        AlterTableFactory.createHandeler(alterOptions).execute(name,connection.getNativeConnection());

//        throw new UnsupportedException("Not yet supported");

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
     * @param typeMetadata the type configure
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

    /**
     * This method create a index.
     *
     * @param indexMetadata the index metadata.
     * @param connection    the connection.
     * @throws UnsupportedException the method is not supporter.
     * @throws ExecutionException   if a fail happen.
     */
    @Override
    protected void createIndex(IndexMetadata indexMetadata, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {

         throw new UnsupportedException("Not yet supported");
    }

    /**
     * This method drop a index.
     *
     * @param indexMetadata the index metadata.
     * @param connection    the connection.
     * @throws UnsupportedException the method is not supporter.
     * @throws ExecutionException   if a fail happen.
     */
    @Override
    protected void dropIndex(IndexMetadata indexMetadata, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Not yet supported");
    }

    /**
     * This method throw a excption.
     *
     * @param originalExcepcion the exception.
     * @param operation         the operation witch throw the excepcion
     * @throws ExecutionException always.
     */
    private void throwHandlerConnectionException(Exception originalExcepcion, String operation)
            throws ExecutionException {
        String msg = "Error find ElasticSearch client in " + operation + ". " + originalExcepcion.getMessage();
        logger.error(msg);
        throw new ExecutionException(msg, originalExcepcion);
    }

    /**
     * This method return the concrete ES Client for a cluster.
     *
     * @param connection the cluster identification.
     * @return the ES Client.
     */
    private Client recoveredClient(Connection connection) throws HandlerConnectionException {
        return (Client) connection.getNativeConnection();
    }

    /**
     * This method crate a elasticsearch Index.
     *
     * @param indexMetaData the metadata.
     * @param connection    the elasticsearch connection.
     * @throws HandlerConnectionException if a exception happen during connection handling.
     * @throws ExecutionException         if an execution error happen.
     */
    private void createESIndex(CatalogMetadata indexMetaData, Connection connection)
            throws HandlerConnectionException, ExecutionException {

        CreateIndexRequestBuilder createIndexRequestBuilder = recoveredClient(connection).admin().indices()
                .prepareCreate(indexMetaData.getName().getName());
        createIndexRequestBuilder.setSettings(transformOptions(indexMetaData));

        createIndexRequestBuilder.execute().actionGet();

    }

    /**
     * This method turn the crossdata metadata into elasticsearch properties.
     *
     * @param indexMetaData the crossdata metadata.
     * @return the elasticsearch options.
     * @throws ExecutionException if a error happens.
     */
    private Map<String, String> transformOptions(CatalogMetadata indexMetaData) throws ExecutionException {

        Map<String, String> transformOptions = new HashMap<>();
        Map<Selector, Selector> options = indexMetaData.getOptions();
        if (options != null) {
            for (Map.Entry<Selector, Selector> key : options.entrySet()) {
                transformOptions
                        .put(SelectorHelper.getValue(String.class, key.getKey()),
                                SelectorHelper.getValue(String.class, key.getValue()));
            }
        }
        return transformOptions;
    }

}

