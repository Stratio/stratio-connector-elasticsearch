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
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.*;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.*;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import static org.mockito.Mockito.mock;

/**
 * QueryBuilder Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * sep 15, 2014
 * </pre>
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {Client.class})
public class ConnectorQueryBuilderTestIT {

    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private static final String COLUMN_NAME = "COLUMN_NAME".toLowerCase();
    private static final String STRING_SELECTOR_VALUE = "string value";
    private static final String COLUMN_1 = "COLUMN_NAME_1".toLowerCase();
    private static final String COLUMN_2 = "COLUMN_NAME_2".toLowerCase();
    private static final String COLUMN_3 = "COLUMN_NAME_3".toLowerCase();
    private static final String CLUSTER_NAME = "CLUSTER_NAME".toLowerCase();
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

        ProjectParsed projectParsed = createProjectParsed();

        //Experimentation
        ActionRequestBuilder searchRequestBuilder = queryBuilder.buildQuery(client, projectParsed);

        //Expectations
        assertNotNull("The request builder is not null", searchRequestBuilder);
        SearchRequest request = (SearchRequest) Whitebox.getInternalState(searchRequestBuilder, "request");
        String[] indices = (String[]) Whitebox.getInternalState(request, "indices");
        assertEquals("the indices length is correct", 1, indices.length);
        assertEquals("The index is correct", INDEX_NAME, indices[0]);
        String[] types = (String[]) Whitebox.getInternalState(request, "types");
        assertEquals("the types length is correct", 1, types.length);
        assertEquals("The types is correct", TYPE_NAME, types[0]);

        SearchSourceBuilder searchSourceBuilder = (SearchSourceBuilder) Whitebox.getInternalState(searchRequestBuilder,
                "sourceBuilder");
        ArrayList fieldNames = (ArrayList) Whitebox.getInternalState(searchSourceBuilder, "fieldNames");
        assertEquals("the fieldNames length is correct", 2, fieldNames.size());
        assertTrue("The fieldNames is correct", fieldNames.contains(COLUMN_1));
        assertTrue("The fieldNames is correct", fieldNames.contains(COLUMN_2));
        assertEquals(5, Whitebox.getInternalState(searchSourceBuilder, "size"));


    }

    private ProjectParsed createProjectParsed() throws ConnectorException {

        ColumnName columnName = new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME);

        Map<Selector, String> alias = new HashMap<>();
        Selector key1 = new ColumnSelector(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_1));
        Selector key2 = new ColumnSelector(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_2));

        alias.put(key1, COLUMN_1);
        alias.put(key2, COLUMN_2);

        Set operations = new HashSet<>();
        operations.add(Operations.FILTER_INDEXED_EQ);
        Select select = new Select(operations, alias, Collections.EMPTY_MAP, Collections.EMPTY_MAP);

        List<ColumnName> columnList = new ArrayList<>();
        columnList.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_1));
        columnList.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_2));
        columnList.add(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_3));

        Set operations2 = new HashSet<>();
        operations2.add(Operations.FILTER_NON_INDEXED_EQ);
        Project project = new Project(operations2, new TableName(INDEX_NAME, TYPE_NAME),
                new ClusterName(CLUSTER_NAME), columnList);

        Relation relation = new Relation(new ColumnSelector(columnName), Operator.EQ, new StringSelector(
                STRING_SELECTOR_VALUE));

        Filter filter = new Filter(operations2, relation);

        project.setNextStep(filter);
        filter.setNextStep(select);
        Limit limit = new Limit(Collections.singleton(Operations.SELECT_LIMIT), 5);
        select.setNextStep(limit);

        ProjectValidator projectValidator = mock(ProjectValidator.class);
        ProjectParsed projectParsed = new ProjectParsed(project, projectValidator);
        return projectParsed;
    }

}
