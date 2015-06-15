package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lcisneros on 2/06/15.
 */
public class MultiMatch extends ESFunction {


    protected MultiMatch(List<Selector> paramareters) {
        super("multi_match", paramareters);

    }

    @Override
    public QueryBuilder buildQuery() {
        String value = getParameters().get(getParameters().size() -1).getStringValue();

        String[] fields = new String[getParameters().size() -1];
        for (int i = 0; i < getParameters().size() -1; i++){
            fields[i]=((ColumnSelector) getParameters().get(i)).getColumnName().getName();
        }

        return QueryBuilders.multiMatchQuery(value, fields);
    }

}