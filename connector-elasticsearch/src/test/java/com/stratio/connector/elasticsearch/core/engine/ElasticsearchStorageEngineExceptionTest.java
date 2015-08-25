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
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * ElasticsearchStorageEngine Tester.
 *
 * @author <Authors name>
 * @version 1.0
 */

public class ElasticsearchStorageEngineExceptionTest {

    private static final String CLUSTER_NAME = "CLUSTER NAME".toLowerCase();
    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final Integer INTEGER_CELL_VALUE = new Integer(5);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private IndexRequestBuilderCreator indexRequestBuilderCreator;
    private ElasticSearchConnectionHandler connectionHandler;
    private Connection<Client> connection;
    private Client client;

    private ElasticsearchStorageEngine elasticsearchStorageEngine;
    private LinkedHashMap<ColumnName, ColumnMetadata> columns = null;
    private Map<Selector, Selector> options = null;
    private Map<IndexName, IndexMetadata> indexes = null;
    private ClusterName clusterRef = null;
    LinkedList<ColumnName> partitionKey = new LinkedList<ColumnName>();
    LinkedList<ColumnName> clusterKey = new LinkedList<ColumnName>();

    @Before
    public void before()  {

        indexRequestBuilderCreator = mock(IndexRequestBuilderCreator.class);
        connectionHandler = mock(ElasticSearchConnectionHandler.class);
        connection = mock(Connection.class);
        client = mock(Client.class);

        elasticsearchStorageEngine = new ElasticsearchStorageEngine(connectionHandler);

        Whitebox.setInternalState(elasticsearchStorageEngine, "indexRequestBuilderCreator", indexRequestBuilderCreator);

    }

    @Test
    public void testInsertExecutionException()
            throws ExecutionException,  UnsupportedException {

        exception.expect(ExecutionException.class);
        exception.expectMessage("Msg");

        when(connectionHandler.getConnection(CLUSTER_NAME)).thenThrow(new ExecutionException("Msg"));

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexes, clusterRef, partitionKey,
                clusterKey);
        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        boolean isNotExists = false;
		elasticsearchStorageEngine.insert(clusterName, targetTable, createRow(ROW_NAME, INTEGER_CELL_VALUE),isNotExists);

    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
