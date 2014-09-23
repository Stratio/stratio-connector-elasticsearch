package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.connector.meta.ElasticsearchResultSet;
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.TableName;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;


/**
 * ConnectorQueryExecutor Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 16, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {Client.class, SearchHits.class, ConnectorQueryExecutor.class, SearchHit.class, SearchHit[].class})
public class ConnectorQueryExecutorTest {


    private static final String TYPE_NAME = "TYPE NAME";
    private static final String INDEX_NAME = "INDEX NAME";
    private static final String COLUMN_NAME = "COLUMN NAME";
    private static final String COLUMN_STRING_VALUE = "COLUMN VALUE";
    private static final java.lang.String SCROLL_ID = "1";

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

        ElasticsearchResultSet resultset = (ElasticsearchResultSet) queryResult.getResultSet();
        assertEquals("The resultset size is correct", 1, resultset.getRows().size());
        Row row = resultset.getRows().get(0);
        assertEquals("The rows number is correct", 1, row.size());
        assertEquals("The value is correct", COLUMN_STRING_VALUE, row.getCells().get(COLUMN_NAME).getValue());

    }


    @Test
    public void testExecuteFetchFilterQuery() throws Exception {

        SearchHits searchHits = mock(SearchHits.class);
        ArrayList<SearchHit> hits = new ArrayList<>();
        hits.add(createHit());
        when(searchHits.iterator()).thenReturn(hits.iterator());


        SearchResponse searchResponse = mock(SearchResponse.class);


        when(searchResponse.getHits()).thenReturn(searchHits);
        SearchHit[] aHits = new SearchHit[0];

        when(searchHits.getHits()).thenReturn(aHits);

        ListenableActionFuture<SearchResponse> listenableActionFuture = mock(ListenableActionFuture.class);

        when(listenableActionFuture.actionGet()).thenReturn(searchResponse);


        Client client = mock(Client.class);


        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        when(requestBuilder.execute()).thenReturn(listenableActionFuture);


        ConnectorQueryData queryData = createQueryData(SearchType.QUERY_THEN_FETCH);

        QueryResult queryResult = connectorQueryExecutor.executeQuery(client, requestBuilder, queryData);

        ElasticsearchResultSet resulset = (ElasticsearchResultSet) queryResult.getResultSet();
        assertEquals("The resulset size is correct", 1, resulset.getRows().size());
        Row row = resulset.getRows().get(0);
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
        Project projection = new Project(Operations.FILTER_INDEXED_EQ,new TableName(INDEX_NAME,TYPE_NAME));
        connectorQueryData.setProjection(projection);


        return connectorQueryData;
    }


}
