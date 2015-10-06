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

package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.commons.engine.query.ProjectValidator;
import com.stratio.crossdata.common.data.*;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.DataType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * ConnectorQueryExecutor Tester.
 *
 * @author <Authors name>
 * @version 1.0
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { Client.class, SearchHits.class, ConnectorQueryExecutor.class, SearchHit.class,
                SearchHit[].class })
public class ConnectorQueryExecutorTest {

    private static final String TYPE_NAME = "TYPE NAME".toLowerCase();
    private static final String INDEX_NAME = "INDEX NAME".toLowerCase();
    private static final String COLUMN_NAME = "COLUMN NAME".toLowerCase();
    public static final String ALIAS = "alias" + COLUMN_NAME;
    private static final String COLUMN_STRING_VALUE = "COLUMN VALUE".toLowerCase();
    private static final java.lang.String SCROLL_ID = "1";
    private static final String CLUSTER_NAME = "CLUSTER_NAME".toLowerCase();

    private ConnectorQueryExecutor connectorQueryExecutor;

    @Before
    public void before() throws Exception {
        connectorQueryExecutor = new ConnectorQueryExecutor();

    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: executeQuery(Client elasticClient, SearchRequestBuilder requestBuilder, ConnectorQueryData queryData)
     */
    @Test
    public void testExecuteScanFilterQuery() throws Exception {

        SearchHits searchHits = mock(SearchHits.class);
        ArrayList<SearchHit> hits = new ArrayList<>();
        hits.add(createHit());
        when(searchHits.iterator()).thenReturn(hits.iterator());

        SearchResponse searchResponse = mock(SearchResponse.class);

        when(searchResponse.getHits()).thenReturn(searchHits);
        SearchHit[] aHits = new SearchHit[1];
        aHits[0] = hits.get(0);

        when(searchHits.getHits()).thenReturn(aHits);

        ListenableActionFuture<SearchResponse> respose = mock(ListenableActionFuture.class);

        when(respose.actionGet()).thenReturn(searchResponse);

        Client client = mock(Client.class);

        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        when(requestBuilder.execute()).thenReturn(respose);

        ProjectParsed projectParsed = createQueryData(SearchType.SCAN);

        //Experimentation
        QueryResult queryResult = connectorQueryExecutor.executeQuery(client, requestBuilder, projectParsed);

        //Expectations
        ResultSet resultset = queryResult.getResultSet();
        assertEquals("The resultset size is correct", 1, resultset.getRows().size());
        Row row = resultset.getRows().get(0);
        assertEquals("The rows number is correct", 1, row.size());
        assertEquals("The value is not correct", COLUMN_STRING_VALUE, row.getCells().get(ALIAS).getValue());

    }

    @Test
    public void testExecuteCountQuery() throws Exception {

        SearchHits searchHits = mock(SearchHits.class);
        ArrayList<SearchHit> hits = new ArrayList<>();
        hits.add(createHit());
        when(searchHits.iterator()).thenReturn(hits.iterator());

        SearchResponse searchResponse = mock(SearchResponse.class);

        when(searchResponse.getHits()).thenReturn(searchHits);
        SearchHit[] aHits = new SearchHit[1];
        aHits[0] = hits.get(0);

        when(searchHits.getHits()).thenReturn(aHits);

        ListenableActionFuture<SearchResponse> respose = mock(ListenableActionFuture.class);

        when(respose.actionGet()).thenReturn(searchResponse);

        Client client = mock(Client.class);

        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        when(requestBuilder.execute()).thenReturn(respose);

        ProjectParsed projectParsed = createQueryData(SearchType.SCAN);

        //Experimentation
        QueryResult queryResult = connectorQueryExecutor.executeQuery(client, requestBuilder, projectParsed);

        //Expectations
        ResultSet resultset = queryResult.getResultSet();
        assertEquals("The resultset size is correct", 1, resultset.getRows().size());
        Row row = resultset.getRows().get(0);
        assertEquals("The rows number is correct", 1, row.size());
        assertEquals("The value is not correct", COLUMN_STRING_VALUE, row.getCells().get(ALIAS).getValue());

    }

//
//
//    @Test
//    public void testExecuteAggregationQuery() throws Exception {
//
//
//
//        //Experimentation
//        QueryResult queryResult = connectorQueryExecutor.executeQuery(client, requestBuilder, projectParsed);
//
//        //Expectations
//        ResultSet resultset = queryResult.getResultSet();
//        assertEquals("The resultset size is correct", 1, resultset.getRows().size());
//        Row row = resultset.getRows().get(0);
//        assertEquals("The rows number is correct", 1, row.size());
//        assertEquals("The value is not correct", "Value 1", row.getCells().get("col1").getValue());
//        assertEquals("The value is not correct", "Value 2", row.getCells().get("col2").getValue());
//
//    }

    private SearchHit createHit() {
        SearchHit searchHit = mock(SearchHit.class);

        Map<String, Object> map = new HashMap<>();
        map.put(COLUMN_NAME, COLUMN_STRING_VALUE);
        when(searchHit.getSource()).thenReturn(map);
        return searchHit;
    }

    private ProjectParsed createQueryData(SearchType searchType) throws ConnectorException {


        Set operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_EQ);

        Project projection = new Project(operations, new TableName(INDEX_NAME, TYPE_NAME),
                        new ClusterName(CLUSTER_NAME));

        Map<Selector, String> column = new HashMap<>();

        ColumnName name = new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME);

        Selector key = new ColumnSelector(name);
        key.setAlias(ALIAS);
        column.put(key, ALIAS);

        Map<String, ColumnType> type = new HashMap<>();
        type.put("alias" + COLUMN_NAME, new ColumnType(DataType.TEXT));
        Map<Selector, ColumnType> typeColumn = new HashMap<>();
        typeColumn.put(key, new ColumnType(DataType.TEXT));

        Set operations2 = new HashSet<>();
        operations2.add(Operations.SELECT_OPERATOR);

        Select select = new Select(operations2, column, type, typeColumn);

        projection.setNextStep(select);

        ProjectParsed projectParsed = new ProjectParsed(projection, mock(ProjectValidator.class));
        return projectParsed;
    }

}
