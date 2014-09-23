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

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.util.reflection.Whitebox;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.utils.IndexRequestBuilderCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.IndexName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;

/**
 * ElasticsearchStorageEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 10, 2014</pre>
 */

public class ElasticsearchStorageEngineExceptionTest {

    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final Integer INTEGER_CELL_VALUE = new Integer(5);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private IndexRequestBuilderCreator indexRequestBuilderCreator;
    private ElasticSearchConnectionHandler connectionHandler;
    private Connection<Client> connection;
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

        indexRequestBuilderCreator = mock(IndexRequestBuilderCreator.class);
        connectionHandler = mock(ElasticSearchConnectionHandler.class);
        connection = mock(Connection.class);
        client = mock(Client.class);

        elasticsearchStorageEngine = new ElasticsearchStorageEngine(connectionHandler);

        Whitebox.setInternalState(elasticsearchStorageEngine, "indexRequestBuilderCreator", indexRequestBuilderCreator);

    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testInsertExecutionException()
            throws ExecutionException, HandlerConnectionException, UnsupportedException {

        exception.expect(ExecutionException.class);
        exception.expectMessage("Error find Connection in CLUSTER NAME. Msg");

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenThrow(new HandlerConnectionException("Msg"));

        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey,
                clusterKey);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        elasticsearchStorageEngine.insert(clusterName, targetTable, createRow(ROW_NAME, INTEGER_CELL_VALUE));

    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
