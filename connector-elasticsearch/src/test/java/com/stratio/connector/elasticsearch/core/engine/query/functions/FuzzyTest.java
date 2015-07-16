package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.FunctionRelation;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lcisneros on 17/06/15.
 */
public class FuzzyTest {
    @Test
    public void testMatch() throws UnsupportedException, ExecutionException {

        List<Selector> parameters = new ArrayList<>();
        TableName tableName = new TableName("catalog", "table");
        parameters.add(new ColumnSelector(new ColumnName(tableName, "colName")));
        parameters.add(new StringSelector("fieldValue"));
        parameters.add(new StringSelector("1"));

        FunctionRelation function = new FunctionRelation(ESFunction.FUZZY, parameters,tableName);
        ESFunction match = ESFunction.build(function);

        //Experimentation
        QueryBuilder builder = match.buildQuery();

        //Expectations
        String expected = "{\"fuzzy\":{\"colName\":{\"value\":\"fieldValue\",\"fuzziness\":\"1\"}}}";
        Assert.assertEquals(expected, builder.toString().replaceAll("\\s+", ""));
    }
}
