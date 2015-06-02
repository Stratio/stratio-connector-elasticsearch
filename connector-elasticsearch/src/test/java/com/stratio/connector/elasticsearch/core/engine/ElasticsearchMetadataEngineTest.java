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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.crossdata.common.data.CatalogName;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.CatalogMetadata;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * ElasticsearchMetadataEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 9, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { Client.class, DeleteIndexResponse.class, ActionResponse.class, ActionFuture.class,
        DeleteMappingResponse.class, XContentBuilder.class, ContentBuilderCreator.class })
public class ElasticsearchMetadataEngineTest {

    public static final String CATALOG_NAME = "catalog_name";
    private static final String TYPE_NAME = "tableName";
    private final String CLUSTER_NAME = "clusterName";
    private final String INDEX_NAME = "indexName";
    ElasticsearchMetadataEngine elasticsearchMetadataEngine;
    @Mock
    ConnectionHandler connectionHandler;
    @Mock
    Connection<Client> connection;
    @Mock
    Client client;
    @Mock
    ContentBuilderCreator deepContentBuilder;

    @Before
    public void before() throws  Exception {

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        elasticsearchMetadataEngine = new ElasticsearchMetadataEngine(connectionHandler);
        Whitebox.setInternalState(elasticsearchMetadataEngine, "contentBuilder", deepContentBuilder);
    }

    /**
     * Method: createCatalog(ClusterName targetCluster, CatalogMetadata indexMetaData)
     */
    @Test
    public void testCreateCatalog() throws Exception {

        ListenableActionFuture<CreateIndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        CreateIndexRequestBuilder createIndexRequestBluilder = mock(CreateIndexRequestBuilder.class);

        when(createIndexRequestBluilder.execute()).thenReturn(listenableActionFuture);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

        when(indicesAdminClient.prepareCreate(CATALOG_NAME)).thenReturn(createIndexRequestBluilder);

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        when(client.admin()).thenReturn(adminClient);

        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<TableName, TableMetadata> tables = Collections.EMPTY_MAP;
        CatalogMetadata catalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME), options, tables);
        elasticsearchMetadataEngine.createCatalog(new ClusterName(CLUSTER_NAME), catalogMetadata);

        verify(createIndexRequestBluilder, times(1)).setSettings(options);
        verify(listenableActionFuture, times(1)).actionGet();
        verify(indicesAdminClient).prepareCreate(CATALOG_NAME);
    }



    @Test
    public void testCreateCatalogINUpper() throws Exception {

        ListenableActionFuture<CreateIndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        CreateIndexRequestBuilder createIndexRequestBluilder = mock(CreateIndexRequestBuilder.class);

        when(createIndexRequestBluilder.execute()).thenReturn(listenableActionFuture);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

        when(indicesAdminClient.prepareCreate(CATALOG_NAME)).thenReturn(createIndexRequestBluilder);

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        when(client.admin()).thenReturn(adminClient);

        Map<Selector, Selector> options = Collections.EMPTY_MAP;
        Map<TableName, TableMetadata> tables = Collections.EMPTY_MAP;
        CatalogMetadata catalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME.toUpperCase()), options, tables);
        elasticsearchMetadataEngine.createCatalog(new ClusterName(CLUSTER_NAME), catalogMetadata);

        verify(createIndexRequestBluilder, times(1)).setSettings(options);
        verify(listenableActionFuture, times(1)).actionGet();
        verify(indicesAdminClient).prepareCreate(CATALOG_NAME);
    }
    @Test
    public void testCreateCatalogWithOptions() throws Exception {

        ListenableActionFuture<CreateIndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        CreateIndexRequestBuilder createIndexRequestBluilder = mock(CreateIndexRequestBuilder.class);

        when(createIndexRequestBluilder.execute()).thenReturn(listenableActionFuture);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);

        when(indicesAdminClient.prepareCreate(CATALOG_NAME)).thenReturn(createIndexRequestBluilder);

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        when(client.admin()).thenReturn(adminClient);

        Map<Selector, Selector> options = new HashMap<>();
        options.put(new StringSelector("key1"), new StringSelector("value1"));

        Map<String, String> optionsTranform = new HashMap<>();
        optionsTranform.put("key1", "value1");

        Map<TableName, TableMetadata> tables = Collections.EMPTY_MAP;
        CatalogMetadata catalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME), options, tables);
        elasticsearchMetadataEngine.createCatalog(new ClusterName(CLUSTER_NAME), catalogMetadata);

        verify(createIndexRequestBluilder, times(1)).setSettings(eq(optionsTranform));
        verify(listenableActionFuture, times(1)).actionGet();
    }

    @Test
    public void testCreateTable() throws Exception {

        TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
        Map<Selector, Selector> options = new HashMap<>();
        java.util.LinkedHashMap<ColumnName, ColumnMetadata> columns = new java.util.LinkedHashMap<>();
        Map<IndexName, IndexMetadata> indexes = new HashMap<IndexName, IndexMetadata>();
        LinkedList<ColumnName> partitionKey = new LinkedList<ColumnName>();
        LinkedList<ColumnName> clusterKey = new LinkedList<ColumnName>();
        
        TableMetadata tableMetadata = new TableMetadata(tableName, options, columns, indexes, 
        		new ClusterName(CLUSTER_NAME), partitionKey, clusterKey);
     		

        XContentBuilder xContentBuilder = mock(XContentBuilder.class);
        when(deepContentBuilder.createTypeSource(tableMetadata)).thenReturn(xContentBuilder);

        PutMappingRequestBuilder putMappingRequest = mock(PutMappingRequestBuilder.class);
        when(putMappingRequest.setIndices(INDEX_NAME)).thenReturn(putMappingRequest);
        when(putMappingRequest.setType(TYPE_NAME)).thenReturn(putMappingRequest);
        when(putMappingRequest.setSource(xContentBuilder)).thenReturn(putMappingRequest);
        ListenableActionFuture<PutMappingResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(putMappingRequest.execute()).thenReturn(listenableActionFuture);

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.preparePutMapping()).thenReturn(putMappingRequest);

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);

        when(client.admin()).thenReturn(adminClient);

        elasticsearchMetadataEngine.createTable(new ClusterName(CLUSTER_NAME), tableMetadata);

        verify(listenableActionFuture, times(1)).actionGet();
    }

    /**
     * Method: dropCatalog(ClusterName targetCluster, CatalogName indexName)
     */
    @Test
    public void testDropCatalog() throws Exception {

        ActionFuture<DeleteIndexResponse> actionFuture = createActionFuture(false, true);

        elasticsearchMetadataEngine.dropCatalog(new ClusterName(CLUSTER_NAME), new CatalogName(INDEX_NAME));

        verify(actionFuture, times(1)).actionGet();
    }

    private ActionFuture<DeleteIndexResponse> createActionFuture(boolean isListenableActionFuture,
            boolean indexResponse) throws Exception {

        DeleteIndexResponse deleteIndexResponse = mock(DeleteIndexResponse.class);
        when(deleteIndexResponse.isAcknowledged()).thenReturn(indexResponse);

        ActionFuture<DeleteIndexResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(deleteIndexResponse);

        DeleteIndexRequest delelteIndexRequest = mock(DeleteIndexRequest.class);
        whenNew(DeleteIndexRequest.class).withParameterTypes(String.class).withArguments(INDEX_NAME)
                .thenReturn(delelteIndexRequest);

        DeleteMappingResponse deletemappingResponse = mock(DeleteMappingResponse.class);
        when(deletemappingResponse.isAcknowledged()).thenReturn(indexResponse);

        ListenableActionFuture<DeleteMappingResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(listenableActionFuture.actionGet()).thenReturn(deletemappingResponse);

        DeleteMappingRequestBuilder deleteMappingRequestBuilder = mock(DeleteMappingRequestBuilder.class);
        when(deleteMappingRequestBuilder.setType(TYPE_NAME)).thenReturn(deleteMappingRequestBuilder);
        when(deleteMappingRequestBuilder.execute()).thenReturn(listenableActionFuture);

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.delete(any(DeleteIndexRequest.class))).thenReturn(actionFuture); //REVIEW
        when(indicesAdminClient.prepareDeleteMapping(INDEX_NAME)).thenReturn(deleteMappingRequestBuilder);

        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);

        ActionFuture returnActionFuture = actionFuture;
        if (isListenableActionFuture) {
            returnActionFuture = listenableActionFuture;
        }
        return returnActionFuture;
    }

    @Test(expected = ExecutionException.class)
    public void testDropCatalogException() throws Exception {

        createActionFuture(false, false);

        elasticsearchMetadataEngine.dropCatalog(new ClusterName(CLUSTER_NAME), new CatalogName(INDEX_NAME));

    }

    /**
     * Method: dropTable(ClusterName targetCluster, TableName typeName)
     */
    @Test
    public void testDropTable() throws Exception {

        ActionFuture<DeleteIndexResponse> actionFuture = createActionFuture(true, true);

        elasticsearchMetadataEngine.dropTable(new ClusterName(CLUSTER_NAME), new TableName(INDEX_NAME, TYPE_NAME));

        verify(actionFuture, times(1)).actionGet();

    }

    @Test(expected = ExecutionException.class)
    public void testDropTableException() throws Exception {

        createActionFuture(true, false);

        elasticsearchMetadataEngine.dropTable(new ClusterName(CLUSTER_NAME), new TableName(INDEX_NAME, TYPE_NAME));

    }

    private void createAdminClient(IndicesAdminClient indicesAdminClient) {
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
    }

}
