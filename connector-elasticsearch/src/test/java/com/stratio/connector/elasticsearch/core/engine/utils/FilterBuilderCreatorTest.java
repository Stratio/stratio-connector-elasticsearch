package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.*;
import org.elasticsearch.index.query.FilterBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by lcisneros on 16/06/15.
 */
public class FilterBuilderCreatorTest {


   private  FilterBuilderCreator filterBuilderCreator;

    @Before
    public void init(){
        filterBuilderCreator = new FilterBuilderCreator();
    }

    @Test
    public void testCreateFilterBuilderEq() throws UnsupportedException, ExecutionException {
        String expectations = "{\"bool\":{\"must\":{\"term\":{\"column\":10}}}}";
        testCreateFilterBuilder(Operator.EQ, expectations);
    }

    @Test
    public void testCreateFilterBuilderDistinc() throws UnsupportedException, ExecutionException {
        String expectations = "{\"bool\":{\"must\":{\"not\":{\"filter\":{\"term\":{\"column\":10}}}}}}";
        testCreateFilterBuilder(Operator.DISTINCT, expectations);
    }

    @Test
    public void testCreateFilterBuilderLT() throws UnsupportedException, ExecutionException {
        String expectations = "{\"bool\":{\"must\":{\"range\":{\"column\":{\"from\":null,\"to\":10,\"include_lower\":true,\"include_upper\":false}}}}}";
        testCreateFilterBuilder(Operator.LT, expectations);
    }

    @Test
    public void testCreateFilterBuilderLET() throws UnsupportedException, ExecutionException {
        String expectations = "{\"bool\":{\"must\":{\"range\":{\"column\":{\"from\":null,\"to\":10,\"include_lower\":true,\"include_upper\":true}}}}}";
        testCreateFilterBuilder(Operator.LET, expectations);
    }

    @Test
    public void testCreateFilterBuilderGT() throws UnsupportedException, ExecutionException {
        String expectations = "{\"bool\":{\"must\":{\"range\":{\"column\":{\"from\":10,\"to\":null,\"include_lower\":false,\"include_upper\":true}}}}}";
        testCreateFilterBuilder(Operator.GT, expectations);
    }

    @Test
    public void testCreateFilterBuilderGET() throws UnsupportedException, ExecutionException {
        String expectations = "{\"bool\":{\"must\":{\"range\":{\"column\":{\"from\":10,\"to\":null,\"include_lower\":true,\"include_upper\":true}}}}}";
        testCreateFilterBuilder(Operator.GET, expectations);
    }

    @Test()
    public void testCreateFilterBuilderFail() throws UnsupportedException, ExecutionException {

        Selector from = new IntegerSelector (1);
        Selector to = new IntegerSelector (10);

        Selector left = new ColumnSelector(new ColumnName(new TableName("catalog", "table"), "column"));
        GroupSelector selector = new GroupSelector(new TableName("catalog", "table"),from, to);

        Relation relations = new Relation(left, Operator.BETWEEN, selector);

        Filter filter = new Filter(new HashSet<>(Arrays.asList(Operations.FILTER_FUNCTION_EQ)), relations);

        //Experimentation
        FilterBuilder result = filterBuilderCreator.createFilterBuilder(Arrays.asList(filter));

        //Expectations
        String expected = "{\"bool\":{\"must\":{\"range\":{\"column\":{\"from\":1,\"to\":10,\"include_lower\":true,\"include_upper\":true}}}}}";
        Assert.assertEquals(expected, result.toString().replace(" ", "").replace("\n", ""));
    }

    private void  testCreateFilterBuilder(Operator operator, String expected) throws UnsupportedException, ExecutionException {

        Selector rigth = new IntegerSelector (10);
        Selector left = new ColumnSelector(new ColumnName(new TableName("catalog", "table"), "column"));

        Relation relations = new Relation(left, operator, rigth);

        Filter filter = new Filter(new HashSet<>(Arrays.asList(Operations.FILTER_FUNCTION_EQ)), relations);

        //Experimentation
        FilterBuilder result = filterBuilderCreator.createFilterBuilder(Arrays.asList(filter));

        //Expectations
        Assert.assertEquals(expected, result.toString().replace(" ", "").replace("\n", ""));
    }
}
