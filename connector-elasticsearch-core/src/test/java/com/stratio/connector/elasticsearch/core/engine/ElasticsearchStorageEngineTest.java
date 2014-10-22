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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.utils.IndexRequestBuilderCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;


/**
 * ElasticsearchStorageEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 10, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
public class ElasticsearchStorageEngineTest {

    private static final String CLUSTER_NAME = "CLUSTER NAME".toLowerCase();
    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME.".toLowerCase();
    private static final String CELL_VALUE = "cell_value";
    private static final Object OTHER_CELL_VALUE = "other cell value";
    private static final Integer INTEGER_CELL_VALUE = new Integer(5);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Mock
    private IndexRequestBuilderCreator indexRequestBuilderCreator;
    @Mock
    private ElasticSearchConnectionHandler connectionHandler;
    @Mock
    private Connection<Client> connection;
    @Mock
    private Client client;

    private ElasticsearchStorageEngine elasticsearchStorageEngine;
    private Map<ColumnName, ColumnMetadata> columns = null;
    private Map<Selector, Selector> options = null;
    private Map<IndexName, IndexMetadata> indexes = null;
    private ClusterName clusterRef = null;
    private List<ColumnName> partirionKey = Collections.emptyList();
    private List<ColumnName> clusterKey = Collections.emptyList();

    @Before
    public void before() throws HandlerConnectionException {

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenReturn(connection);
        when(connection.getNativeConnection()).thenReturn(client);
        elasticsearchStorageEngine = new ElasticsearchStorageEngine(connectionHandler);

        Whitebox.setInternalState(elasticsearchStorageEngine, "indexRequestBuilderCreator", indexRequestBuilderCreator);

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: insert(ClusterName targetCluster, TableMetadata targetTable, Row row)
     */
    @Test
    public void testInsertOne() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey,
                clusterKey);
        Row row = createRow(ROW_NAME, CELL_VALUE);

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row))
                .thenReturn(indexRequestBuilder);

        ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);

        elasticsearchStorageEngine.insert(clusterName, targetTable, row);

        verify(listenableActionFuture, times(1)).actionGet();

    }

    @Test
    public void testInserBulk()
            throws UnsupportedException, ExecutionException, java.util.concurrent.ExecutionException,
            InterruptedException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey,
                clusterKey);
        Collection<Row> row = new ArrayList<>();
        Row row1 = createRow(ROW_NAME, CELL_VALUE);
        row.add(row1);
        Row row2 = createRow(OTHER_ROW_NAME, OTHER_CELL_VALUE);
        row.add(row2);

        IndexRequestBuilder indexRequestBuilder1 = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row1))
                .thenReturn(indexRequestBuilder1);
        IndexRequestBuilder indexRequestBuilder2 = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row2))
                .thenReturn(indexRequestBuilder2);

        BulkResponse bulkResponse = mock(BulkResponse.class);
        when(bulkResponse.hasFailures()).thenReturn(false);

        ListenableActionFuture<BulkResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(listenableActionFuture.actionGet()).thenReturn(bulkResponse);

        BulkRequestBuilder bulkRequesBuilder = mock(BulkRequestBuilder.class);
        when(bulkRequesBuilder.execute()).thenReturn(listenableActionFuture);

        when(client.prepareBulk()).thenReturn(bulkRequesBuilder);

        elasticsearchStorageEngine.insert(clusterName, targetTable, row);

        verify(bulkRequesBuilder, times(1)).add(indexRequestBuilder1);
        verify(bulkRequesBuilder, times(1)).add(indexRequestBuilder2);
        verify(listenableActionFuture, times(1)).actionGet();

    }

    @Test
    public void testInsertOnePK() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, ROW_NAME));

        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey,
                clusterKey);
        Row row = createRow(ROW_NAME, CELL_VALUE);

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row))
                .thenReturn(indexRequestBuilder);

        ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);

        elasticsearchStorageEngine.insert(clusterName, targetTable, row);

        verify(listenableActionFuture, times(1)).actionGet();

    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
