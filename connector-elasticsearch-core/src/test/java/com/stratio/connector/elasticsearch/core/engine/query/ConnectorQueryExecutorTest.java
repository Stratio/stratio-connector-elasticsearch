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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * ConnectorQueryExecutor Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 16, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(
        value = { Client.class, SearchHits.class, ConnectorQueryExecutor.class, SearchHit.class, SearchHit[].class })
public class ConnectorQueryExecutorTest {

    private static final String TYPE_NAME = "TYPE NAME".toLowerCase();
    private static final String INDEX_NAME = "INDEX NAME".toLowerCase();
    private static final String COLUMN_NAME = "COLUMN NAME".toLowerCase();
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
        when(searchResponse.getScrollId()).thenReturn(SCROLL_ID);

        when(searchResponse.getHits()).thenReturn(searchHits);
        SearchHit[] aHits = new SearchHit[0];

        when(searchHits.getHits()).thenReturn(aHits);

        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);

        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);

        SearchScrollRequestBuilder searchScrollRequestBuilder = mock(SearchScrollRequestBuilder.class);
        when(searchScrollRequestBuilder.setScroll(any(TimeValue.class))).thenReturn(searchScrollRequestBuilder);
        when(searchScrollRequestBuilder.execute()).thenReturn(listenableActionFuture);
        Client client = mock(Client.class);
        when(client.prepareSearchScroll(SCROLL_ID)).thenReturn(searchScrollRequestBuilder);

        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        when(requestBuilder.execute()).thenReturn(listenableActionFuture);

        ConnectorQueryData queryData = createQueryData(SearchType.SCAN);

        QueryResult queryResult = connectorQueryExecutor.executeQuery(client, requestBuilder, queryData);

        ResultSet resultset = queryResult.getResultSet();
        assertEquals("The resultset size is correct", 1, resultset.getRows().size());
        Row row = resultset.getRows().get(0);
        assertEquals("The rows number is correct", 1, row.size());
        assertEquals("The value is correct", COLUMN_STRING_VALUE, row.getCells().get(COLUMN_NAME).getValue());

    }

    private SearchHit createHit() {
        SearchHit searchHit = mock(SearchHit.class);

        Map<String, Object> map = new HashMap<>();
        map.put(COLUMN_NAME, COLUMN_STRING_VALUE);
        when(searchHit.getSource()).thenReturn(map);
        return searchHit;
    }

    private ConnectorQueryData createQueryData(SearchType searchType) {
        ConnectorQueryData connectorQueryData = new ConnectorQueryData();

        connectorQueryData.setSearchType(searchType);
        Project projection = new Project(Operations.FILTER_INDEXED_EQ, new TableName(INDEX_NAME, TYPE_NAME),
                new ClusterName(CLUSTER_NAME));
        connectorQueryData.setProjection(projection);

        Map<ColumnName, String> column = new HashMap<>();
        column.put(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME), COLUMN_NAME);

        Map<String, ColumnType> type = new HashMap<>();
        type.put(COLUMN_NAME, ColumnType.TEXT);
        Select select = new Select(Operations.SELECT_OPERATOR, column, type);
        connectorQueryData.setSelect(select);
        return connectorQueryData;
    }

}
