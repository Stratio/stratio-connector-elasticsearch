package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

/**
 * Created by lcisneros on 2/06/15.
 */
public class MatchPhrase extends ESFunction {

    protected MatchPhrase(List<Selector> paramareters) {
        super("match_phrase", paramareters);

    }

    @Override
    public QueryBuilder buildQuery() {
        String field = ((ColumnSelector) getParameters().get(0)).getColumnName().getName();
        String value = getParameters().get(1).getStringValue();
        return QueryBuilders.matchPhraseQuery(field, value.toLowerCase());
    }

}
