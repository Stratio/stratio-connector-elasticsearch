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

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.utils.IndexRequestBuilderCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.QueryBuilderCreator;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.*;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

/**
 * ElasticsearchStorageEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 10, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ElasticsearchStorageEngine.class, QueryBuilderCreator.class })
public class ElasticsearchStorageEngineTest {

    private static final String CLUSTER_NAME = "CLUSTER NAME".toLowerCase();
    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String COLUMN_NAME = "column_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME.".toLowerCase();
    private static final String CELL_VALUE = "cell_value";
    private static final Object OTHER_CELL_VALUE = "other cell value";

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
    private LinkedHashMap<ColumnName, ColumnMetadata> columns = null;
    private Map<Selector, Selector> options = null;
    private Map<IndexName, IndexMetadata> indexes = null;
    private ClusterName clusterRef = null;
    LinkedList<ColumnName> partitionKey = new LinkedList<ColumnName>();
    LinkedList<ColumnName> clusterKey = new LinkedList<ColumnName>();

    @Before
    public void before() throws ExecutionException {

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

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexes, clusterRef, partitionKey,
                clusterKey);
        Row row = createRow(COLUMN_NAME, CELL_VALUE);

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row))
                .thenReturn(indexRequestBuilder);

        ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);

        boolean isNotExists = false;
		elasticsearchStorageEngine.insert(clusterName, targetTable, row, isNotExists);

        verify(listenableActionFuture, times(1)).actionGet();

    }

    @Test
    public void testInserBulk()
            throws UnsupportedException, ExecutionException, java.util.concurrent.ExecutionException,
            InterruptedException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexes, clusterRef, partitionKey,
                clusterKey);
        Collection<Row> row = new ArrayList<>();
        Row row1 = createRow(COLUMN_NAME, CELL_VALUE);
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

        boolean isNotExists = false;
		elasticsearchStorageEngine.insert(clusterName, targetTable, row, isNotExists);

        verify(bulkRequesBuilder, times(1)).add(indexRequestBuilder1);
        verify(bulkRequesBuilder, times(1)).add(indexRequestBuilder2);
        verify(listenableActionFuture, times(1)).actionGet();

    }

    @Test
    public void testInsertOnePK() throws UnsupportedException, ExecutionException {

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);


        partitionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexes, clusterRef, partitionKey,
                clusterKey);
        Row row = createRow(COLUMN_NAME, CELL_VALUE);

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row))
                .thenReturn(indexRequestBuilder);

        ListenableActionFuture<IndexResponse> listenableActionFuture = mock(ListenableActionFuture.class);
        when(indexRequestBuilder.execute()).thenReturn(listenableActionFuture);

        boolean isNotExists = false;
		elasticsearchStorageEngine.insert(clusterName, targetTable, row, isNotExists);

        verify(listenableActionFuture, times(1)).actionGet();

    }

    @Test
    public void testDelete() throws Exception {
        Collection<Filter> whereFilter = new ArrayList<>();
        Set operations = new HashSet<>();
        operations.add(Operations.DELETE_PK_EQ);
        whereFilter.add(new Filter(operations, new Relation(new ColumnSelector(new ColumnName
                (INDEX_NAME, TYPE_NAME, COLUMN_NAME)),
                Operator.EQ,
                new StringSelector("1"))));

        QueryBuilderCreator queryBuilderCreator = mock(QueryBuilderCreator.class);
        QueryBuilder queryBuilder = mock(QueryBuilder.class);
        when(queryBuilderCreator.createBuilder(whereFilter)).thenReturn(queryBuilder);
        whenNew(QueryBuilderCreator.class).withNoArguments().thenReturn(queryBuilderCreator);

        ListenableActionFuture<DeleteByQueryResponse> listeneableActionFure = mock(ListenableActionFuture.class);

        DeleteByQueryRequestBuilder deleteByquery = mock(DeleteByQueryRequestBuilder.class);
        when(client.prepareDeleteByQuery(INDEX_NAME)).thenReturn(deleteByquery);
        when(deleteByquery.setQuery(queryBuilder)).thenReturn(deleteByquery);

        when(deleteByquery.execute()).thenReturn(listeneableActionFure);
        elasticsearchStorageEngine.delete(new TableName(INDEX_NAME, TYPE_NAME), whereFilter, connection);

        verify(listeneableActionFure).actionGet();

    }

    @Test
    public void testTruncate() throws Exception {
        Collection<Filter> whereFilter = Collections.EMPTY_LIST;

        QueryBuilderCreator queryBuilderCreator = mock(QueryBuilderCreator.class);
        QueryBuilder queryBuilder = mock(QueryBuilder.class);
        when(queryBuilderCreator.createBuilder(whereFilter)).thenReturn(queryBuilder);
        whenNew(QueryBuilderCreator.class).withNoArguments().thenReturn(queryBuilderCreator);

        ListenableActionFuture<DeleteByQueryResponse> listeneableActionFure = mock(ListenableActionFuture.class);

        DeleteByQueryRequestBuilder deleteByquery = mock(DeleteByQueryRequestBuilder.class);
        when(client.prepareDeleteByQuery(INDEX_NAME)).thenReturn(deleteByquery);
        when(deleteByquery.setQuery(queryBuilder)).thenReturn(deleteByquery);

        when(deleteByquery.execute()).thenReturn(listeneableActionFure);
        elasticsearchStorageEngine.delete(new TableName(INDEX_NAME, TYPE_NAME), whereFilter, connection);

        verify(listeneableActionFure).actionGet();
    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
