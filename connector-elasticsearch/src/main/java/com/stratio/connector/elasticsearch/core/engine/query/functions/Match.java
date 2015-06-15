package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

/**
 * Created by lcisneros on 2/06/15.
 */
public class Match extends ESFunction{



    protected Match(List<Selector> paramareters) {
        super("contains", paramareters);

    }

    @Override
    public QueryBuilder buildQuery() {
        String field = ((ColumnSelector) getParameters().get(0)).getColumnName().getName();
        String value = getParameters().get(1).getStringValue();
        return QueryBuilders.matchQuery(field, value.toLowerCase());
    }
}
