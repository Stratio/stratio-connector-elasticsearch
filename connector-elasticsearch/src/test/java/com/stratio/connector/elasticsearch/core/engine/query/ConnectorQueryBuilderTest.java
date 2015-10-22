package com.stratio.connector.elasticsearch.core.engine.query;


import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.elasticsearch.core.engine.query.functions.ESFunction;
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

            Collection<Filter> filters = new ArrayList<>();
            filters.add(buildFilter(COLUMN_1, "search string"));

            when(parsedQuery.getFilter()).thenReturn(filters);

            Collection<FunctionFilter> functionFilters = new ArrayList<FunctionFilter>();
            when(parsedQuery.getFunctionFilters()).thenReturn(functionFilters);

            // Mocks return fields property
            buildReturnFields(parsedQuery);

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


    private Filter buildFilter(String field, String value){
        ColumnName columnName = new ColumnName(INDEX, TYPE, field);
        Relation relation = new Relation(new ColumnSelector(columnName), Operator.EQ, new StringSelector(value));
        Set<Operations> operations = new HashSet<Operations>();
        operations.add(Operations.FILTER_NON_INDEXED_EQ);

        return new Filter(operations, relation);
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


    /**
     * select column1, column2 from table where column1 = "val1" or column2 = "val2" limit 5
     */
    @Test
    public void testORQueryCase1(){
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":5,\"query\":"+
                "{\"filtered\":{\"query\":{\"match_all\":{}},"+
                "\"filter\":"+
                "{\"bool\":{\"should\":[" +
                        "{\"bool\":{\"must\":{\"term\":{\"column1\":\"val1\"}}}}," +
                        "{\"bool\":{\"must\":{\"term\":{\"column2\":\"val2\"}}}}]}}}},"+
                "\"fields\":[\"column1\",\"column2\"]}");

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            Collection<Filter> matches = new ArrayList<Filter>();
            when(parsedQuery.getMatchList()).thenReturn(matches);

            List<List<ITerm>> terms = new ArrayList<>();

            terms.add(Collections.singletonList(((ITerm) buildFilter(COLUMN_1, "Val1"))));
            terms.add(Collections.singletonList(((ITerm) buildFilter(COLUMN_2, "Val2"))));

            Disjunction ds = new Disjunction(Collections.singleton(Operations.FILTER_DISJUNCTION), terms);

            when(parsedQuery.getDisjunctionList()).thenReturn(Collections.singleton(ds));


            // Mocks return fields property
            buildReturnFields(parsedQuery);

            //Experimentation
            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);

            //Expectations
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }


    /**
     * select column1, column2 from table where (column1 = "val1" or column1 = "val2" limit 5) AND (column2 = "val3" or column2 = "val4" limit 5)
     */
    @Test
    public void testORQueryCase2(){
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":5," +
                "\"query\":{" +
                "\"filtered\":{\"query\":{\"match_all\":{}}," +
                "\"filter\":{\"bool\":{" +
                    "\"must\":[" +
                        "{\"bool\":{\"should\":[{\"term\":{\"column1\":\"val1\"}},{\"term\":{\"column1\":\"val2\"}}]}}," +
                        "{\"bool\":{\"should\":[{\"term\":{\"column2\":\"val3\"}},{\"term\":{\"column2\":\"val4\"}}]}}" +
                    "]}}}}," +
                "\"fields\":[\"column1\",\"column2\"]}");

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            Collection<Filter> matches = new ArrayList<Filter>();
            when(parsedQuery.getMatchList()).thenReturn(matches);

            List<List<ITerm>> terms = new ArrayList<>();
            terms.add(Collections.singletonList(((ITerm) buildFilter(COLUMN_1, "Val1"))));
            terms.add(Collections.singletonList(((ITerm) buildFilter(COLUMN_1, "Val2"))));

            List<Disjunction> disjunctions = new ArrayList<>();
            disjunctions.add(new Disjunction(Collections.singleton(Operations.FILTER_DISJUNCTION), terms));

            terms = new ArrayList<>();
            terms.add(Collections.singletonList(((ITerm) buildFilter(COLUMN_2, "Val3"))));
            terms.add(Collections.singletonList(((ITerm) buildFilter(COLUMN_2, "Val4"))));

            disjunctions.add(new Disjunction(Collections.singleton(Operations.FILTER_DISJUNCTION), terms));

            when(parsedQuery.getDisjunctionList()).thenReturn(disjunctions);


            // Mocks return fields property
            buildReturnFields(parsedQuery);

            //Experimentation
            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);

            //Expectations
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }


    /**
     * select column1, column2 from table
     * where (column1 = "val1" or column1 = "val2") OR (column2 = "val3" AND column3 = "val4" limit 5)
     */
    @Test
    public void testORQueryCase3(){
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":5," +
                "\"query\":{\"filtered\":{\"query\":{\"match_all\":{}}," +
                "\"filter\":" +
                    "{\"bool\":{\"should\":[{\"bool\":" +
                        "{\"should\":{\"bool\":{\"must\":[{\"term\":{\"column1\":\"val1\"}},{\"term\":{\"column1\":\"val2\"}}]}}}}," +
                        "{\"bool\":{\"must\":[{\"term\":{\"column2\":\"val4\"}},{\"term\":{\"column2\":\"val3\"}}]}}]}}}}," +
                "\"fields\":[\"column1\",\"column2\"]}");

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            Collection<Filter> matches = new ArrayList<Filter>();
            when(parsedQuery.getMatchList()).thenReturn(matches);

            List<List<ITerm>> terms = new ArrayList<>();

            //OR
            List<ITerm> orTerms = new ArrayList<>();
            orTerms.add(buildFilter(COLUMN_1, "Val1"));
            orTerms.add( buildFilter(COLUMN_1, "Val2"));

            List<ITerm> disjunctions = new ArrayList<>();
            disjunctions.add(new Disjunction(Collections.singleton(Operations.FILTER_DISJUNCTION),Collections.singletonList(orTerms)));
            terms.add(disjunctions);


            //And
            List<ITerm> andTerms = new ArrayList<>();
            andTerms.add(buildFilter(COLUMN_2, "Val4"));
            andTerms.add(buildFilter(COLUMN_2, "Val3"));
            terms.add(andTerms);

            //Main OR
            Collection mainDisjunction = Collections.singleton(new Disjunction(Collections.singleton(Operations.FILTER_DISJUNCTION), terms));
            when(parsedQuery.getDisjunctionList()).thenReturn(mainDisjunction);

            // Mocks return fields property
            buildReturnFields(parsedQuery);

            //Experimentation
            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);

            //Expectations
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }



    @Test
    public void testFunction(){
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":5," +
                "\"query\":{\"bool\":" +
                    "{\"must\":{\"match\":{\"colName\":{\"query\":\"value\",\"type\":\"boolean\",\"minimum_should_match\":\"100%\"}}}}}," +
                "\"fields\":[\"column1\",\"column2\"]}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            Collection<Filter> matches = new ArrayList<Filter>();
            when(parsedQuery.getMatchList()).thenReturn(matches);

            Collection<FunctionFilter> functionFilters = new ArrayList<>();
            functionFilters.add(buildFunctionContainsFilter("colName", "value"));

            when(parsedQuery.getFunctionFilters()).thenReturn(functionFilters);

            // Mocks return fields property
            buildReturnFields(parsedQuery);

            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }


    @Test
    public void testFunctionWithAnd(){
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":5," +
                        "\"query\":{\"bool\":" +
                        "{\"must\":[" +
                        "{\"match\":{\"colName\":{\"query\":\"value\",\"type\":\"boolean\",\"minimum_should_match\":\"100%\"}}}," +
                        "{\"match\":{\"colName2\":{\"query\":\"value2\",\"type\":\"boolean\",\"minimum_should_match\":\"100%\"}}}]}}," +
                        "\"fields\":[\"column1\",\"column2\"]}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            when(parsedQuery.getMatchList()).thenReturn(new ArrayList<Filter>());

            Collection<FunctionFilter> functionFilters = new ArrayList<>();
            functionFilters.add(buildFunctionContainsFilter("colName", "value"));
            functionFilters.add(buildFunctionContainsFilter("colName2", "value2"));

            when(parsedQuery.getFunctionFilters()).thenReturn(functionFilters);

            // Mocks return fields property
            buildReturnFields(parsedQuery);

            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    @Test
    public void testFunctionWithOR(){
        final String EXPECTED_QUERY = cleanJson(
                "{\"size\":5," +
                        "\"query\":{\"bool\":" +
                        "{\"must\":{\"bool\":" +
                            "{\"should\":[" +
                            "{\"bool\":{\"must\":{\"match\":{\"colName\":{\"query\":\"value\",\"type\":\"boolean\",\"minimum_should_match\":\"100%\"}}}}}," +
                            "{\"bool\":{\"must\":{\"match\":{\"colName2\":{\"query\":\"value2\",\"type\":\"boolean\",\"minimum_should_match\":\"100%\"}}}}}]}}}}," +
                        "\"fields\":[\"column1\",\"column2\"]}"
        );

        try {
            ProjectParsed parsedQuery = createBaseParsedQuery();

            // Mocks limit property
            addLimit(5, parsedQuery);

            // Mocks query properties
            when(parsedQuery.getMatchList()).thenReturn(new ArrayList<Filter>());

            List<List<ITerm>> terms = new ArrayList<>();

            terms.add(Collections.singletonList(((ITerm) buildFunctionContainsFilter("colName", "value"))));
            terms.add(Collections.singletonList(((ITerm) buildFunctionContainsFilter("colName2", "value2"))));

            Disjunction ds = new Disjunction(Collections.singleton(Operations.FILTER_DISJUNCTION), terms);

            when(parsedQuery.getDisjunctionOfFunctionsList()).thenReturn(Collections.singleton(ds));

            // Mocks return fields property
            buildReturnFields(parsedQuery);

            SearchRequestBuilder requestBuilder = (SearchRequestBuilder) connectorQueryBuilder.buildQuery(client, parsedQuery);
            assertEquals(EXPECTED_QUERY, cleanJson(requestBuilder.toString()));
        }
        catch (Exception exception) {
            fail(exception.getMessage());
        }
    }

    public void buildReturnFields(ProjectParsed parsedQuery) {
        List<String> returnFields = new ArrayList<String>();
        returnFields.add(COLUMN_1);
        returnFields.add(COLUMN_2);
        addReturnFields(returnFields, parsedQuery);
    }

    public FunctionFilter buildFunctionContainsFilter(String columnName, String value) {
        List<Selector> parameters = new ArrayList<>();
        TableName tableName = new TableName("catalog", "table");
        parameters.add(new ColumnSelector(new ColumnName(tableName, columnName)));
        parameters.add(new StringSelector(value));
        parameters.add(new StringSelector("100%"));

        FunctionRelation function = new FunctionRelation(ESFunction.CONTAINS, parameters,tableName);

        Set<Operations> operations = new HashSet();
        operations.add(Operations.FILTER_FUNCTION);

        return new FunctionFilter(operations, function);
    }

}