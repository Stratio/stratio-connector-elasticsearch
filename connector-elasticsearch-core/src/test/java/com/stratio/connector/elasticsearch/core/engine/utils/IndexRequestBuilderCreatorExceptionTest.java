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

package com.stratio.connector.elasticsearch.core.engine.utils;


import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;

import java.util.*;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * IndexRequestBuilderCreator Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 12, 2014</pre>
 */
public class IndexRequestBuilderCreatorExceptionTest {


    private static final String CLUSTER_NAME = "CLUSTER NAME";
    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private TableName tableMame = new TableName(INDEX_NAME, TYPE_NAME);
    private static final String ROW_NAME = "row_name";
    private static final String OTHER_ROW_NAME = "OTHER_ROW_NAME";
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
    public void testCreateIndesRequestTwoPK() throws UnsupportedException, ExecutionException {

        exception.expect(UnsupportedException.class);
        exception.expectMessage("Only one PK is allowed");


        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, ROW_NAME));
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, OTHER_ROW_NAME));

        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey, clusterKey);
        Row row = createRow(ROW_NAME, CELL_VALUE);
        row.addCell(OTHER_ROW_NAME, new Cell(CELL_VALUE));

        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        when(indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row)).thenReturn(indexRequestBuilder);


        indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);


    }

    @Test
    public void testCreateIndesRequestIntegerPK() throws UnsupportedException, ExecutionException {

        exception.expect(UnsupportedException.class);
        exception.expectMessage("The PK only can has String values");

        ClusterName clusterName = new ClusterName(CLUSTER_NAME);

        partirionKey = new ArrayList<>();
        partirionKey.add(new ColumnName(INDEX_NAME, TYPE_NAME, ROW_NAME));


        TableMetadata targetTable = new TableMetadata(tableMame, options, columns, indexes, clusterRef, partirionKey, clusterKey);
        Row row = createRow(ROW_NAME, INTEGER_CELL_VALUE);


        IndexRequestBuilder indexRequestBuilder = mock(IndexRequestBuilder.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ROW_NAME, INTEGER_CELL_VALUE);


        indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);


    }


    private Row createRow(String rowKey, Object cellValue) {
        Cell cell = new Cell(cellValue);
        Row row = new Row(rowKey, cell);
        return row;
    }


}
