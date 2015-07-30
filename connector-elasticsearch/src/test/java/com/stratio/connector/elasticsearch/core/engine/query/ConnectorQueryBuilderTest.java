package com.stratio.connector.elasticsearch.core.engine.query;


import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConnectorQueryBuilderTest {

    // Constants
    private final String INDEX = "index";
    private final String TYPE = "type";

    final String COLUMN_1 = "column1";
    final String COLUMN_2 = "column2";

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
    public void testNullClient(){
        ProjectParsed parsedQuery = mock(ProjectParsed.class);

        try {connectorQueryBuilder.buildQuery(null, parsedQuery);}
        catch (Exception exception) {assertTrue(exception instanceof ExecutionException);}
    }


    @Test
    public void testNullQuery(){
        try {connectorQueryBuilder.buildQuery(client, null);}
        catch (Exception exception) {assertTrue(exception instanceof ExecutionException);}
    }


    @Test
    public void testMatchAllQuery(){
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
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }


    @Test
    public void testComplexQuery(){
        final String EXPECTED_QUERY = cleanJson(
            "{" +
                "\"size\" : 5," +
                "\"query\" : {" +
                    "\"filtered\" : {" +
                        "\"query\" : {" +
                            "\"match_all\" : { }" +
                        "}," +
                        "\"filter\" : {" +
                            "\"bool\" : {" +
                                "\"must\" : {" +
                                    "\"term\" : {" +
                                        "\"column1\" : \"search string\"" +
                                    "}" +
                                "}" +
                            "}" +
                        "}" +
                    "}" +
                "}," +
                "\"fields\" : [ \"column1\", \"column2\" ]" +
            "}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            Collection<Filter> matches = new ArrayList<Filter>();
            when(parsedQuery.getMatchList()).thenReturn(matches);

            Collection<Filter> filters = new ArrayList<Filter>();
            addFilter(COLUMN_1, "search string", filters);
            when(parsedQuery.getFilter()).thenReturn(filters);

            Collection<FunctionFilter> functionFilters = new ArrayList<FunctionFilter>();
            when(parsedQuery.getFunctionFilters()).thenReturn(functionFilters);

            // Mocks return fields property
            List<String> returnFields = new ArrayList<String>();
            returnFields.add(COLUMN_1);
            returnFields.add(COLUMN_2);
            addReturnFields(returnFields, parsedQuery);

            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }


    private ProjectParsed createBaseParsedQuery (){
        ProjectParsed parsedQuery = mock(ProjectParsed.class);
        when(parsedQuery.getProject()).thenReturn(projection);

        Collection<Filter> matches = new ArrayList<>();
        when(parsedQuery.getMatchList()).thenReturn(matches);

        Collection<Filter> filters = new ArrayList<>();
        when(parsedQuery.getFilter()).thenReturn(filters);

        Collection<FunctionFilter> functionFilters = new ArrayList<>();
        when(parsedQuery.getFunctionFilters()).thenReturn(functionFilters);

        return parsedQuery;
    }


    private void addLimit (int size, ProjectParsed parsedQuery){
        Limit limit = mock(Limit.class);
        when(limit.getLimit()).thenReturn(5);
        when(parsedQuery.getLimit()).thenReturn(limit);
    }


    private void addFilter (String field, String value, Collection<Filter> filters){
        ColumnName columnName = new ColumnName(INDEX, TYPE, field);
        Relation relation = new Relation(new ColumnSelector(columnName), Operator.EQ, new StringSelector("search string"));
        Set<Operations> operations = new HashSet<Operations>();
        operations.add(Operations.FILTER_NON_INDEXED_EQ);
        Filter filter = new Filter(operations, relation);

        filters.add(filter);
    }


    private void addReturnFields (List<String> returnFields, ProjectParsed parsedQuery){
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


    private String cleanJson (String json){
        return json.replaceAll("\\s", "");
    }
}