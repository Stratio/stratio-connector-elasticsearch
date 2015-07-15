package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;


public class MultiMatchFuzzy extends ESFunction {

    protected MultiMatchFuzzy(List<Selector> parameters) {
        super(ESFunction.MULTI_MATCH_FUZZY, parameters);
    }

    @Override
    public QueryBuilder buildQuery() {

        String fuzziness = getParameters().get(getParameters().size() -1).getStringValue();
        String value = getParameters().get(getParameters().size() -2).getStringValue();
        String[] fields = new String[getParameters().size() -2];

        for (int i = 0; i < getParameters().size() -2; i++){
            if (getParameters().get(i) instanceof ColumnSelector){
                fields[i]=((ColumnSelector) getParameters().get(i)).getColumnName().getName();
            }else{
                fields[i]= getParameters().get(i).getStringValue();
            }
        }

        return QueryBuilders.multiMatchQuery(value, fields).fuzziness(fuzziness);
    }
}
