package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * LogicalPlanExecutor Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 15, 2014</pre>
 */
public class ConnectorQueryParserTest {

    private static final String INDEX_NAME = "INDEX_NAME";
    private static final String TYPE_NAME = "TYPE_NAME";
    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String STRING_COLUMN_VALUE = "STRING_COLUMN_VALUE";
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


        LogicalWorkflow logicalWorkflow = createLogicalWorkFlow();

        ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(logicalWorkflow);

        Collection<Filter> filters = queryData.getFilter();


        assertNotNull("The filter is not null", filters);
        assertEquals("The filter size is correct", 1, filters.size());
        Filter filter = (Filter) filters.toArray()[0];

        assertEquals("The filter operation is correct", Operations.FILTER_FUNCTION_EQ, filter.getOperation());
        assertEquals("The left term is type correct,", ColumnSelector.class.getCanonicalName(), filter.getRelation().getLeftTerm().getClass().getCanonicalName());
        assertEquals("The filter table is correct", TYPE_NAME, ((ColumnSelector) filter.getRelation().getLeftTerm()).getName().getTableName().getName());
        assertEquals("The operator is correct", Operator.EQ, filter.getRelation().getOperator());
        assertEquals("The right term is correct", STRING_COLUMN_VALUE, ((StringSelector) filter.getRelation().getRightTerm()).getValue());


        Project projection = queryData.getProjection();
        assertNotNull("The projection is not null", projection);
        assertEquals("The type in the projection is correct", TYPE_NAME, projection.getTableName().getName());
        assertEquals("The index in the projection is correct", INDEX_NAME, projection.getTableName().getCatalogName().getName());


    }


    private LogicalWorkflow createLogicalWorkFlow() {


        Operations operations = Operations.FILTER_FUNCTION_EQ;

        List<LogicalStep> initalSteps = new ArrayList<>();

        Relation filterRelation = new Relation(new ColumnSelector(new ColumnName(INDEX_NAME, TYPE_NAME, COLUMN_NAME)), Operator.EQ, new StringSelector(STRING_COLUMN_VALUE));
        initalSteps.add(new Filter(operations, filterRelation));

        initalSteps.add(new Project(operations, new TableName(INDEX_NAME, TYPE_NAME)));

        LogicalWorkflow logicalWorkflow = new LogicalWorkflow(initalSteps);

        return logicalWorkflow;
    }


}
