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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * LogicalPlanExecutor Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 15, 2014</pre>
 */
public class ConnectorQueryParserTest {

    private static final String INDEX_NAME = "INDEX_NAME".toLowerCase();
    private static final String TYPE_NAME = "TYPE_NAME".toLowerCase();
    private static final String COLUMN_NAME = "COLUMN_NAME".toLowerCase();
    private static final String STRING_COLUMN_VALUE = "STRING_COLUMN_VALUE".toLowerCase();
    private static final String CLUSTER_NAME = "CLUSTER_NAME".toLowerCase();
    ConnectorQueryParser queryParser;

    @Before
    public void before() throws Exception {

        queryParser = new ConnectorQueryParser();
    }

    @After
    public void after() throws Exception {
    }

    /**
     * Method: transformLogicalWorkFlow(LogicalWorkflow logicalPlan)
     */
    @Test
    public void testTransformSimpleWorkFlow() throws Exception {

        Project logicalWorkflow = createLogicalWorkFlow();

        ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(logicalWorkflow);

        Collection<Filter> filters = queryData.getFilter();

        assertNotNull("The filter is not null", filters);
        assertEquals("The filter size is correct", 1, filters.size());
        Filter filter = (Filter) filters.toArray()[0];

        assertEquals("The filter operation is correct", Operations.FILTER_FUNCTION_EQ, filter.getOperation());
        assertEquals("The left term is type correct,", ColumnSelector.class.getCanonicalName(),
                filter.getRelation().getLeftTerm().getClass().getCanonicalName());
        assertEquals("The filter table is correct", TYPE_NAME,
                ((ColumnSelector) filter.getRelation().getLeftTerm()).getName().getTableName().getName());
        assertEquals("The operator is correct", Operator.EQ, filter.getRelation().getOperator());
        assertEquals("The right term is correct", STRING_COLUMN_VALUE,
                ((StringSelector) filter.getRelation().getRightTerm()).getValue());

        Project projection = queryData.getProjection();
        assertNotNull("The projection is not null", projection);
        assertEquals("The type in the projection is correct", TYPE_NAME, projection.getTableName().getName());
        assertEquals("The index in the projection is correct", INDEX_NAME,
                projection.getTableName().getCatalogName().getName());

    }

    private Project createLogicalWorkFlow() {

        Operations operations = Operations.FILTER_FUNCTION_EQ;

        List<LogicalStep> initalSteps = new ArrayList<>();

        ColumnName columnName = new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME);
        Relation filterRelation = new Relation(new ColumnSelector(columnName),
                Operator.EQ, new StringSelector(STRING_COLUMN_VALUE));
        Filter filter = new Filter(operations, filterRelation);

        Project project = new Project(operations, new TableName(INDEX_NAME, TYPE_NAME), new ClusterName(CLUSTER_NAME));
        project.setNextStep(filter);
        initalSteps.add(project);

        Map<ColumnName, String> column = new HashMap<>();
        column.put(columnName,  "alias"+COLUMN_NAME);

        Map<String, ColumnType> type = new HashMap<>();
        type.put("alias"+COLUMN_NAME, ColumnType.TEXT);
        Map<ColumnName, ColumnType> typeColumName = new HashMap<>();
        typeColumName.put(columnName, ColumnType.TEXT);
        Select select = new Select(Operations.SELECT_OPERATOR, column, type,typeColumName);
        filter.setNextStep(select);

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(initalSteps);

        return project;
    }

}
