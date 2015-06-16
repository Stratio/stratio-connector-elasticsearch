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

package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * IndexRequestBuilderCreator Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * sep 12, 2014
 * </pre>
 */

public class IndexRequestBuilderCreatorExceptionTest {

    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME";
    private static final String CELL_VALUE = "cell_value";

    @Rule
    public ExpectedException exception = ExpectedException.none();
    IndexRequestBuilderCreator indexRequestBuilderCreator;
    private LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
    private Map<Selector, Selector> options = new LinkedHashMap<>();
    private Map<IndexName, IndexMetadata> indexes = new HashMap<IndexName, IndexMetadata>();
    private ClusterName clusterRef = new ClusterName(CLUSTER_NAME);
    private LinkedList<ColumnName> partitionKey = new LinkedList<ColumnName>();
    private LinkedList<ColumnName> clusterKey = new LinkedList<ColumnName>();

    @Mock
    private ElasticSearchConnectionHandler connectionHandler;
    @Mock
    private Connection<Client> connection;
    @Mock
    private Client client;

    @Before
    public void before() throws Exception {

        client = mock(Client.class);
        indexRequestBuilderCreator = new IndexRequestBuilderCreator();
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testCreateIndesRequestTwoPK() throws UnsupportedException, ExecutionException {

        exception.expect(ExecutionException.class);
        exception.expectMessage("Only one PK is allowed");

        partitionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, ROW_NAME));
        partitionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, OTHER_ROW_NAME));

        TableMetadata targetTable = new TableMetadata(tableName, options, columns, indexes, clusterRef, partitionKey,
                        clusterKey);
        Row row = createRow(ROW_NAME, CELL_VALUE);
        row.addCell(OTHER_ROW_NAME, new Cell(CELL_VALUE));

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row)).thenReturn(
                        indexRequestBuilder);

        indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);

    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
