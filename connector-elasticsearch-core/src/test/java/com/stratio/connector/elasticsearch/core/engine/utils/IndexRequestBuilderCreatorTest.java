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

package com.stratio.connector.elasticsearch.core.engine.utils;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.selectors.Selector;

/**
 * IndexRequestBuilderCreator Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 12, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { Client.class })
public class IndexRequestBuilderCreatorTest {

    private static final String CLUSTER_NAME = "CLUSTER NAME".toLowerCase();
    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String COLUMN_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME".toLowerCase();
    private static final String CELL_VALUE = "cell_value";

    private static final Integer INTEGER_CELL_VALUE = new Integer(5);
    @Rule
    public ExpectedException exception = ExpectedException.none();
    IndexRequestBuilderCreator indexRequestBuilderCreator;
    private Map<ColumnName, ColumnMetadata> columns = null;
    private Map<Selector, Selector> options = null;
    private Map<IndexName, IndexMetadata> indexes = null;
    private ClusterName clusterRef = null;
    private List<ColumnName> partirionKey = Collections.emptyList();
    private List<ColumnName> clusterKey = Collections.emptyList();
    @Mock
    private ElasticSearchConnectionHandler connectionHandler;
    @Mock
    private Connection<Client> connection;
    @Mock
    private Client client;

    @Before
    public void before() throws Exception {
        indexRequestBuilderCreator = new IndexRequestBuilderCreator();
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void createIndexRequestBuilderTest() throws UnsupportedException {

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));
        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey,
                clusterKey);
        Row row = createRow(COLUMN_NAME, CELL_VALUE);
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));

        Map<String, Object> dataInsert = new HashMap<>();
        dataInsert.put(COLUMN_NAME, CELL_VALUE);

        Map other = new HashMap<>();
        other.put(COLUMN_NAME, CELL_VALUE);

        IndexRequestBuilder indexRequiestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequiestBuilder.setSource(eq(other))).thenReturn(indexRequiestBuilder);
        when(client.prepareIndex(INDEX_NAME, TYPE_NAME, CELL_VALUE)).thenReturn(indexRequiestBuilder);

        IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator
                .createIndexRequestBuilder(targetTable, client, row);

        assertNotNull("The index request builder is not null", indexRequestBuilder);

    }

    @Test
    public void createIndexRequestBuilderWithPkTest() throws UnsupportedException {

        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey,
                clusterKey);
        Row row = createRow(COLUMN_NAME, CELL_VALUE);
        row.addCell(COLUMN_NAME, new Cell(CELL_VALUE));

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME));

        Map<String, Object> dataInsert = new HashMap<>();
        dataInsert.put(COLUMN_NAME, CELL_VALUE);

        Map other = new HashMap<>();
        other.put(COLUMN_NAME, CELL_VALUE);

        IndexRequestBuilder indexRequiestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequiestBuilder.setSource(eq(other))).thenReturn(indexRequiestBuilder);
        when(client.prepareIndex(INDEX_NAME, TYPE_NAME)).thenReturn(indexRequiestBuilder);

        IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator
                .createIndexRequestBuilder(targetTable, client, row);

        assertNotNull("The index request builder is not null", indexRequestBuilder);

    }

    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }

}
