package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class Fuzzy extends ESFunction{

    protected Fuzzy(List<Selector> paramareters) {
        super(ESFunction.FUZZY, paramareters);
    }

    @Override
    public QueryBuilder buildQuery() {
        String field = ((ColumnSelector) getParameters().get(0)).getColumnName().getName();
        String value = getParameters().get(1).getStringValue();
        String fuzzynest = getParameters().get(2).getStringValue();

        return QueryBuilders.fuzzyQuery(field, value.toLowerCase()).fuzziness(Fuzziness.build(fuzzynest));
    }


}
