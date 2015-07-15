package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

public class Fuzzy extends ESFunction{

    protected Fuzzy(List<Selector> parameters) {
        super(ESFunction.FUZZY, parameters);
    }

    @Override
    public QueryBuilder buildQuery() {

        String field = "";
        if (getParameters().get(0) instanceof ColumnSelector ){
            field = ((ColumnSelector) getParameters().get(0)).getColumnName().getName();
        } else {
            field = getParameters().get(0).getStringValue();
        }

        String value = getParameters().get(1).getStringValue();
        String fuzziness = getParameters().get(2).getStringValue();

        return QueryBuilders.fuzzyQuery(field, value).fuzziness(Fuzziness.build(fuzziness));
    }
}
