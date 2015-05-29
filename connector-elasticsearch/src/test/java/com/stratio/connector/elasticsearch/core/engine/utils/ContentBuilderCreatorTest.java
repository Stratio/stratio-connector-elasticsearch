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

import static org.junit.Assert.assertEquals;

import java.util.*;

import com.stratio.crossdata.common.metadata.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.IndexName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * DeepContentBuilder Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 11, 2014</pre>
 */
public class ContentBuilderCreatorTest {

    public static final String INDEX_NAME = "index";
    public static final String TYPE_NAME = "type";
    public static final String CLUSTER_NAME = "CLUSTER_NAME";
    private static final String RESULT_CREATE_TABLE = "{\"_id\":{\"index\":\"not_analyzed\"}," +
            "\"properties\":{\"column_1\":{\"type\":\"long\",\"index\":\"analyzed\"},\"column_2\":{\"type\":\"boolean\",\"index\":\"not_analyzed\"},\"column_3\":{\"type\":\"double\",\"index\":\"analyzed\"},\"column_4\":{\"type\":\"float\",\"index\":\"analyzed\"},\"column_5\":{\"type\":\"integer\",\"index\":\"analyzed\"},\"column_6\":{\"type\":\"string\",\"index\":\"analyzed\"},\"column_7\":{\"type\":\"string\",\"index\":\"analyzed\"}}}";
    ContentBuilderCreator deepContentBuilder;

    @Before
    public void before() throws Exception {

        deepContentBuilder = new ContentBuilderCreator();
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: createTypeSource(TableMetadata typeMetadata)
     */

    @Test
    public void testCreateTable() throws Exception {
        TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
        Map<Selector, Selector> options = new LinkedHashMap<>();
        options.put(new StringSelector("number_of_shards"), new IntegerSelector(5));
        options.put(new StringSelector("number_of_replicas"), new IntegerSelector(2));
        options.put(new StringSelector("other"), new StringSelector("String value"));
        

        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        columns.putAll(creteColumns("column_1", new ColumnType(DataType.BIGINT)));
        columns.putAll(creteColumns("column_2", new ColumnType(DataType.BOOLEAN)));
        columns.putAll(creteColumns("column_3", new ColumnType(DataType.DOUBLE)));
        columns.putAll(creteColumns("column_4", new ColumnType(DataType.FLOAT)));
        columns.putAll(creteColumns("column_5", new ColumnType(DataType.INT)));
        columns.putAll(creteColumns("column_6", new ColumnType(DataType.TEXT)));
        columns.putAll(creteColumns("column_7", new ColumnType(DataType.VARCHAR)));
        Map<IndexName, IndexMetadata> indexes = new HashMap<IndexName, IndexMetadata>();

        ClusterName clusterRef = new ClusterName(CLUSTER_NAME);

        
        LinkedList<ColumnName> partitionKey = new LinkedList<ColumnName>();
        LinkedList<ColumnName> clusterKey = new LinkedList<ColumnName>();
        
        TableMetadata tableMetadata = new TableMetadata(tableName, options, columns, indexes, 
        		clusterRef, partitionKey, clusterKey);
        
        

        assertEquals("The JSON is not correct", RESULT_CREATE_TABLE,
                (deepContentBuilder.createTypeSource(tableMetadata).string()));

    }

    private Map<ColumnName, ColumnMetadata> creteColumns(String columnName, ColumnType columnType) {
        Map<ColumnName, ColumnMetadata> columns = new HashMap<>();
        ColumnName column = new ColumnName(INDEX_NAME, TYPE_NAME, columnName);
        columns.put(column, new ColumnMetadata(column, null, columnType));

        return columns;
    }


    @Test
    public void testCreateTableWithColumnOptions() throws Exception {
        TableName tableName = new TableName(INDEX_NAME, TYPE_NAME);
        Map<Selector, Selector> options = new LinkedHashMap<>();
        options.put(new StringSelector("number_of_shards"), new IntegerSelector(5));
        options.put(new StringSelector("number_of_replicas"), new IntegerSelector(2));
        options.put(new StringSelector("other"), new StringSelector("String value"));


        LinkedHashMap<ColumnName, ColumnMetadata> columns = new LinkedHashMap<>();
        columns.putAll(creteColumns("column_1", new ColumnType(DataType.BIGINT)));
        Map<String, List<String>> columnPropertie = new HashMap<>();
        columnPropertie.put("analyzer", Arrays.asList("aaa", "bbb"));
        columnPropertie.put("format", Arrays.asList("aebi"));
        columns.putAll(creteColumns("column_6", new ColumnType(DataType.TEXT, columnPropertie)));
        Map<IndexName, IndexMetadata> indexes = new HashMap<IndexName, IndexMetadata>();

        ClusterName clusterRef = new ClusterName(CLUSTER_NAME);


        LinkedList<ColumnName> partitionKey = new LinkedList<ColumnName>();
        LinkedList<ColumnName> clusterKey = new LinkedList<ColumnName>();

        TableMetadata tableMetadata = new TableMetadata(tableName, options, columns, indexes,
                clusterRef, partitionKey, clusterKey);

        //Experientation
        XContentBuilder result = deepContentBuilder.createTypeSource(tableMetadata);


        //Expectations
        String expected = "{\"_id\":{\"index\":\"not_analyzed\"},\"" +
                "properties\":{\"column_1\":{\"type\":\"long\",\"index\":\"analyzed\"},\"" +
                "column_6\":{\"type\":\"string\",\"index\":\"analyzed\",\"" +
                "analyzer\":[\"aaa\",\"bbb\"],\"format\":[\"aebi\"]}}}";

        assertEquals("The JSON is not correct", expected, result.string());

    }
}
