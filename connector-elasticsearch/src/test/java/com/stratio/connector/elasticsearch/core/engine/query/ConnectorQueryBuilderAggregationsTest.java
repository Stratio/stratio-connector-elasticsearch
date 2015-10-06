package com.stratio.connector.elasticsearch.core.engine.query;


import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectorQueryBuilderAggregationsTest {

    // Constants
    private final String INDEX = "index";
    private final String TYPE = "type";

    // Variables
    private ConnectorQueryBuilder connectorQueryBuilder;
    private Client client;
    private Project projection;


    @Before
    public void before() throws Exception {
        connectorQueryBuilder = new ConnectorQueryBuilder();

        client = mock(Client.class);
        SearchRequestBuilder requestBuilder = new SearchRequestBuilder(client);
        when(client.prepareSearch()).thenReturn(requestBuilder);

        projection = mock(Project.class);
        TableName tableName = mock(TableName.class);
        when(projection.getCatalogName()).thenReturn(INDEX);
        when(projection.getTableName()).thenReturn(tableName);
        when(tableName.getName()).thenReturn(TYPE);
    }


    @Test
    public void testMatchAllQuery() {
        final String EXPECTED_QUERY = cleanJson(
                "{" +
                        "\"query\" : {" +
                        "\"match_all\" : {}" +
                        "}" +
                        "}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();
            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        } catch (Exception exception) {
            fail(exception.getMessage());
        }
    }


    @Test
    public void testGroupByTwoField() {
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":0,\"query\":" +
                        "{\"match_all\":{}}," +
                        "\"aggregations\":{" +
                            "\"colA\":{\"terms\":{\"field\":\"colA\",\"size\":5}," +
                            "\"aggregations\":{\"colB\":{\"terms\":{\"field\":\"colB\"}}}}}}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            addLimit(5, parsedQuery);

            List<String> returnFields = new ArrayList<>();
            returnFields.add("colA");
            returnFields.add("colB");
            addReturnFields(parsedQuery, returnFields);

            Selector colA = new ColumnSelector((new ColumnName(INDEX, TYPE, "colA")));
            Selector colB = new ColumnSelector((new ColumnName(INDEX, TYPE, "colB")));
            GroupBy groupBy = new GroupBy(Collections.singleton(Operations.SELECT_GROUP_BY),Arrays.asList(colA, colB));
            when(parsedQuery.getGroupBy()).thenReturn(groupBy);


            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        } catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    @Test
    public void testGroupByThreeField() {
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":0,\"query\":" +
                        "{\"match_all\":{}}," +
                        "\"aggregations\":{" +
                        "\"colA\":{\"terms\":{\"field\":\"colA\",\"size\":5}," +
                        "\"aggregations\":{\"colB\":{\"terms\":{\"field\":\"colB\",\"size\":5}," +
                            "\"aggregations\":{\"colC\":{\"terms\":{\"field\":\"colC\"}" +
                        "}}}}}}}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            addLimit(5, parsedQuery);

            List<String> returnFields = new ArrayList<>();
            returnFields.add("colA");
            returnFields.add("colB");
            returnFields.add("colC");
            addReturnFields(parsedQuery, returnFields);

            Selector colA = new ColumnSelector((new ColumnName(INDEX, TYPE, "colA")));
            Selector colB = new ColumnSelector((new ColumnName(INDEX, TYPE, "colB")));
            Selector colC = new ColumnSelector((new ColumnName(INDEX, TYPE, "colC")));
            GroupBy groupBy = new GroupBy(Collections.singleton(Operations.SELECT_GROUP_BY),Arrays.asList(colA, colB, colC));
            when(parsedQuery.getGroupBy()).thenReturn(groupBy);


            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        } catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    @Test
    public void testGroupByThreeFieldWuthOrder() {
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":0,\"query\":" +
                        "{\"match_all\":{}}," +
                        "\"aggregations\":{" +
                        "\"colA\":{\"terms\":{\"field\":\"colA\",\"size\":5,\"order\":{\"_term\":\"asc\"}}," +
                        "\"aggregations\":{\"colB\":{\"terms\":{\"field\":\"colB\",\"size\":5}," +
                        "\"aggregations\":{\"colC\":{\"terms\":{\"field\":\"colC\"}" +
                        "}}}}}}}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            addLimit(5, parsedQuery);


            List<String> returnFields = new ArrayList<>();
            returnFields.add("colA");
            returnFields.add("colB");
            returnFields.add("colC");
            addReturnFields(parsedQuery, returnFields);

            Selector colA = new ColumnSelector((new ColumnName(INDEX, TYPE, "colA")));
            Selector colB = new ColumnSelector((new ColumnName(INDEX, TYPE, "colB")));
            Selector colC = new ColumnSelector((new ColumnName(INDEX, TYPE, "colC")));
            GroupBy groupBy = new GroupBy(Collections.singleton(Operations.SELECT_GROUP_BY),Arrays.asList(colA, colB, colC));
            when(parsedQuery.getGroupBy()).thenReturn(groupBy);


            OrderByClause orderByClause = new OrderByClause(OrderDirection.ASC, colA);
            OrderBy orderBy = new OrderBy(Collections.singleton(Operations.SELECT_ORDER_BY), Arrays.asList(orderByClause));
            when(parsedQuery.getOrderBy()).thenReturn(orderBy);

            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        } catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    private ProjectParsed createBaseParsedQuery() {
        ProjectParsed parsedQuery = mock(ProjectParsed.class);
        when(parsedQuery.getProject()).thenReturn(projection);
        when(parsedQuery.getMatchList()).thenReturn(new ArrayList<Filter>());
        when(parsedQuery.getFilter()).thenReturn(new ArrayList<Filter>());
        when(parsedQuery.getFunctionFilters()).thenReturn(new ArrayList<FunctionFilter>());

        return parsedQuery;
    }


    private void addLimit(int size, ProjectParsed parsedQuery) {
        Limit limit = mock(Limit.class);
        when(limit.getLimit()).thenReturn(5);
        when(parsedQuery.getLimit()).thenReturn(limit);
    }


    private String cleanJson(String json) {
        return json.replaceAll("\\s", "");
    }


    private void addReturnFields(ProjectParsed parsedQuery, List<String> returnFields) {
        Map<Selector, String> selectors = new LinkedHashMap<Selector, String>();
        for (String returnField : returnFields) {
            Selector selector = mock(Selector.class);
            selectors.put(selector, returnField);
            when(selector.getColumnName()).thenReturn(new ColumnName(INDEX, TYPE, returnField));
        }
        Select select = mock(Select.class);
        when(select.getColumnMap()).thenReturn(selectors);
        when(parsedQuery.getSelect()).thenReturn(select);
    }


}