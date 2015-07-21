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

import com.stratio.connector.commons.TimerJ;
import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.CommonsMetadataEngine;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.elasticsearch.core.engine.metadata.AlterTableFactory;
import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible of managing the ElasticSearch Metadata.
 *
 * @author darroyo
 */
public class ElasticsearchMetadataEngine extends CommonsMetadataEngine<Client> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ContentBuilderCreator contentBuilder = new ContentBuilderCreator();

    /**
     * Constructor.
     *
     * @param connectionHandler the connector handler.
     */
    public ElasticsearchMetadataEngine(ConnectionHandler connectionHandler) {
        super(connectionHandler);
    }

    @Override
    @TimerJ
    protected List<CatalogMetadata> provideMetadata(ClusterName targetCluster, Connection<Client> connection) throws ConnectorException {
        throw new UnsupportedException("Operation provideCatalogMetadata: Not supported yet by ElasticSearch");
    }

    @Override
    @TimerJ
    protected CatalogMetadata provideCatalogMetadata(CatalogName catalogName, ClusterName targetCluster, Connection<Client> connection) throws ConnectorException {
        throw new UnsupportedException("Operation provideCatalogMetadata: Not supported yet by ElasticSearch");
    }

    @Override
    @TimerJ
    protected TableMetadata provideTableMetadata(TableName tableName, ClusterName targetCluster, Connection<Client> connection) throws ConnectorException {
        throw new UnsupportedException("Operation provideTableMetadata: Not supported yet by ElasticSearch");

    }

    @Override
    @TimerJ
    protected void alterCatalog(CatalogName catalogName, Map<Selector, Selector> options, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Operation alterCatalog: Not supported yet by ElasticSearch");
    }

    @Override
    @TimerJ
    protected void alterTable(TableName name, AlterOptions alterOptions, Connection<Client> connection) throws UnsupportedException, ExecutionException {
        AlterTableFactory.createHandler(alterOptions).execute(name, connection.getNativeConnection());
    }

    @Override
    @TimerJ
    protected void createCatalog(CatalogMetadata catalogMetadata, Connection<Client> connection) throws UnsupportedException, ExecutionException {
        createESIndex(catalogMetadata, connection);
    }

    /**
     * This method creates a type in ES.
     *
     * @param tableMetadata the type configure
     * @throws UnsupportedException if any operation is not supported.
     * @throws ExecutionException   if an error occur.
     */
    @Override
    @TimerJ
    protected void createTable(TableMetadata tableMetadata, Connection<Client> connection) throws UnsupportedException, ExecutionException {
        String indexName = tableMetadata.getName().getCatalogName().getName();
        XContentBuilder xContentBuilder = contentBuilder.createTypeSource(tableMetadata);

            IndicesAdminClient indicesClient= recoveredClient(connection).admin().indices();

            indicesClient
                    .preparePutMapping()
                    .setIndices(indexName)
                    .setType(tableMetadata.getName().getName())
                    .setSource(xContentBuilder)
                    .execute()
                    .actionGet();

    }

    /**
     * This method drops an index in ES.
     *
     * @param name the index name.
     */

    @Override
    @TimerJ
    protected void dropCatalog(CatalogName name, Connection<Client> connection) throws UnsupportedException, ExecutionException {

        DeleteIndexResponse delete = null;

        delete = recoveredClient(connection).admin().indices().delete(new DeleteIndexRequest(name.getName()))
                .actionGet();
        if (!delete.isAcknowledged()) {
            throw new ExecutionException("dropCatalog request has not been acknowledged");
        }

    }

    /**
     * This method drops a type in ES.
     *
     * @param name the type name.
     */

    @Override
    @TimerJ
    protected void dropTable(TableName name, Connection<Client> connection) throws UnsupportedException, ExecutionException {

        DeleteMappingResponse delete = null;
        delete = recoveredClient(connection).admin().indices()
                .prepareDeleteMapping(name.getCatalogName().getName()).setType(name.getName())
                .execute().actionGet();
        if (!delete.isAcknowledged()) {
            throw new ExecutionException("dropTable request has not been acknowledged");
        }
    }

    /**
     * This method creates an index.
     *
     * @param indexMetadata the index metadata.
     * @param connection    the connection.
     * @throws UnsupportedException the method is not supporter.
     */

    @Override
    @TimerJ
    protected void createIndex(IndexMetadata indexMetadata, Connection<Client> connection) throws UnsupportedException, ExecutionException {

        throw new UnsupportedException("Operation createIndex: Not supported yet by ElasticSearch");

    }

    /**
     * This method drops an index.
     *
     * @param indexMetadata the index metadata.
     * @param connection    the connection.
     * @throws UnsupportedException the method is not supported.
     */

    @Override
    @TimerJ
    protected void dropIndex(IndexMetadata indexMetadata, Connection<Client> connection) throws UnsupportedException, ExecutionException {

        throw new UnsupportedException("Operation dropIndex: Not supported yet by ElasticSearch");

    }

    /**
     * This method returns the concrete ES Client of a cluster.
     *
     * @param connection the cluster identification.
     * @return the ES Client.
     */

    @TimerJ
    private Client recoveredClient(Connection connection) {
        return (Client) connection.getNativeConnection();
    }

    /**
     * This method creates an Elasticsearch Index.
     *
     * @param indexMetaData the metadata.
     * @param connection    the elasticsearch connection.
     * @throws ExecutionException if an execution error happen.
     */
    @TimerJ
    private void createESIndex(CatalogMetadata indexMetaData, Connection connection) throws ExecutionException {

        IndicesAdminClient client = recoveredClient(connection).admin().indices();
        if (!client.exists(new IndicesExistsRequest(indexMetaData.getName().getName())).actionGet().isExists()){
            String index = indexMetaData.getName().getName();

            CreateIndexRequestBuilder createIndexRequestBuilder = client.prepareCreate(index);

            Map<String, Object> settings = transformOptions(indexMetaData);
            final String JSON_SETTINGS_KEY = "settings";

            if (settings.containsKey(JSON_SETTINGS_KEY)){
                String settingsJson = settings.remove(JSON_SETTINGS_KEY).toString();
                if (!settings.isEmpty()){
                    throw new ExecutionException("You can either define the \"settings\" property including complete settings " +
                            "or other properties for specific settings, but not both.");
                }
                createIndexRequestBuilder.setSettings(settingsJson);
            }
            else {createIndexRequestBuilder.setSettings(settings);}

            createIndexRequestBuilder.execute().actionGet();
        }
    }

    /**
     * This method turns the Crossdata metadata into Elasticsearch properties.
     *
     * @param indexMetaData the Crossdata metadata.
     * @return the Elasticsearch options.
     * @throws ExecutionException if an error occurs.
     */
    @TimerJ
    private Map<String, Object> transformOptions(CatalogMetadata indexMetaData) throws ExecutionException {

        Map<String, Object> transformOptions = new HashMap<>();
        Map<Selector, Selector> options = indexMetaData.getOptions();
        if (options != null) {
            for (Map.Entry<Selector, Selector> key : options.entrySet()) {
                transformOptions.put(SelectorHelper.getValue(String.class, key.getKey()),
                        SelectorHelper.getValue(String.class, key.getValue()));
            }
        }
        return transformOptions;
    }

}
