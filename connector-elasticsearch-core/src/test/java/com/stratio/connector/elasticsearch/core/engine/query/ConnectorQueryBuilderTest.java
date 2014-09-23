package com.stratio.connector.elasticsearch.core.engine.query;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.internal.util.reflection.Whitebox;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

/**
 * QueryBuilder Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 15, 2014</pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { Client.class })
public class ConnectorQueryBuilderTest {

    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String STRING_SELECTOR_VALUE = "string value";
    private static final String COLUMN_1 = "COLUMN_NAME_1";
    private static final String COLUMN_2 = "COLUMN_NAME_2";
    private static final String COLUMN_3 = "COLUMN_NAME_3";
    static Client client;
    ConnectorQueryBuilder queryBuilder;

    @BeforeClass
    public static void beforeClass() throws Exception {

        NodeBuilder nodeBuilder = nodeBuilder();

        Node node = nodeBuilder.node();
        client = node.client();
    }

    @Before
    public void before() throws Exception {

        queryBuilder = new ConnectorQueryBuilder();
        NodeBuilder nodeBuilder = nodeBuilder();

        Node node = nodeBuilder.node();
        client = node.client();
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: buildQuery(Client elasticClient, QueryData queryData)
     */
    @Test
    public void testBuildQuery() throws Exception {

        ConnectorQueryData queryData = createQueryData();

        SearchRequestBuilder searchRequestBuilder = queryBuilder.buildQuery(client, queryData);

        assertNotNull("The request builder is not null", searchRequestBuilder);
        SearchRequest request = (SearchRequest) Whitebox.getInternalState(searchRequestBuilder, "request");
        String[] indices = (String[]) Whitebox.getInternalState(request, "indices");
        assertEquals("the indices length is correct", 1, indices.length);
        assertEquals("The index is correct", INDEX_NAME, indices[0]);
        String[] types = (String[]) Whitebox.getInternalState(request, "types");
        assertEquals("the types length is correct", 1, types.length);
        assertEquals("The types is correct", TYPE_NAME, types[0]);

        SearchSourceBuilder searchSourceBuilder = (SearchSourceBuilder) Whitebox
                .getInternalState(searchRequestBuilder, "sourceBuilder");
        ArrayList fieldNames = (ArrayList) Whitebox.getInternalState(searchSourceBuilder, "fieldNames");
        assertEquals("the fieldNames length is correct", 2, fieldNames.size());
        assertTrue("The fieldNames is correct", fieldNames.contains(COLUMN_1));
        assertTrue("The fieldNames is correct", fieldNames.contains(COLUMN_2));

    }

    private ConnectorQueryData createQueryData() {
        ConnectorQueryData queryData = new ConnectorQueryData();
        ColumnName columnName = new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME);
        Relation relation = new Relation(new ColumnSelector(columnName), Operator.EQ,
                new StringSelector(STRING_SELECTOR_VALUE));

        queryData.addFilter(new Filter(Operations.FILTER_NON_INDEXED_EQ, relation));
        Map<String, String> alias = new HashMap<>();
        alias.put(COLUMN_1, COLUMN_1);
        alias.put(COLUMN_2, COLUMN_2);
        queryData.setSelect(new Select(Operations.FILTER_INDEXED_EQ, alias));
        List<ColumnName> columnList = new ArrayList<>();
        columnList.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_1));
        columnList.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_2));
        columnList.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_3));
        queryData.setProjection(
                new Project(Operations.FILTER_NON_INDEXED_EQ, new TableName(INDEX_NAME, TYPE_NAME), columnList));

        return queryData;
    }

}
