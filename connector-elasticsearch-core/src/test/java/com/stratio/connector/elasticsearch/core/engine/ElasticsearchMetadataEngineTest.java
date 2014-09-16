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


import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * ElasticsearchMetadataEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 9, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {Client.class, DeleteIndexResponse.class, ActionResponse.class, ActionFuture.class, DeleteMappingResponse.class, XContentBuilder.class, ContentBuilderCreator.class})
public class ElasticsearchMetadataEngineTest {


    public static final String CATALOG_NAME = "CATALOG_NAME";
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
    public void before() throws HandlerConnectionException, Exception {


        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        elasticsearchMetadataEngine = new ElasticsearchMetadataEngine(connectionHandler);
        Whitebox.setInternalState(elasticsearchMetadataEngine, "deepContentBuilder", deepContentBuilder);
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


        Map<String, Object> options = Collections.EMPTY_MAP;
        Map<TableName, TableMetadata> tables = Collections.EMPTY_MAP;
        CatalogMetadata catalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME), options, tables);
        elasticsearchMetadataEngine.createCatalog(new ClusterName(CLUSTER_NAME), catalogMetadata);

        verify(createIndexRequestBluilder, times(1)).setSettings(options);
        verify(listenableActionFuture, times(1)).actionGet();
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


        Map<String, Object> options = new HashMap<>();
        options.put("key1", "value1");
        Map<TableName, TableMetadata> tables = Collections.EMPTY_MAP;
        CatalogMetadata catalogMetadata = new CatalogMetadata(new CatalogName(CATALOG_NAME), options, tables);
        elasticsearchMetadataEngine.createCatalog(new ClusterName(CLUSTER_NAME), catalogMetadata);

        verify(createIndexRequestBluilder, times(1)).setSettings(options);
        verify(listenableActionFuture, times(1)).actionGet();
    }


    @Test
    public void testCreateTable() throws Exception {


        TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
        TableMetadata tableMetadata = new TableMetadata(tableName, null, null, null, null, null, null);


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


        Map<String, Object> options = Collections.EMPTY_MAP;


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

    private ActionFuture<DeleteIndexResponse> createActionFuture(boolean isListenableActionFuture, boolean indexResponse) throws Exception {


        DeleteIndexResponse deleteIndexResponse = mock(DeleteIndexResponse.class);
        when(deleteIndexResponse.isAcknowledged()).thenReturn(indexResponse);

        ActionFuture<DeleteIndexResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(deleteIndexResponse);

        DeleteIndexRequest delelteIndexRequest = mock(DeleteIndexRequest.class);
        whenNew(DeleteIndexRequest.class).withParameterTypes(String.class).withArguments(INDEX_NAME).thenReturn(delelteIndexRequest);

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
