package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.statements.structures.AliasSelector;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Created by lcisneros on 25/09/15.
 */
public class RowSorter implements Comparator<Row> {
    private List<OrderByClause> sortCriteria;
    public RowSorter(List<OrderByClause> criteria){
        this.sortCriteria = criteria;
    }

    @Override
    public int compare(Row o1, Row o2) {
        Integer value1 = null;


        Iterator<OrderByClause> columns = sortCriteria.iterator();

        while(columns.hasNext()){
            OrderByClause clause = columns.next();

            String fieldName = SelectorUtils.calculateAlias(clause.getSelector());

            if (!o1.getCells().containsKey(fieldName)){
                return 0;
            }

            Comparable o1Value = (Comparable) o1.getCell(fieldName).getValue();
            Comparable o2Value = (Comparable) o2.getCell(fieldName).getValue();
            if (clause.getDirection().equals(OrderDirection.ASC)) {
                value1 = o1Value.compareTo(o2Value);
            }else{
                value1 = o2Value.compareTo(o1Value);
            }

            if (value1 != 0){
                return value1;
            }
        }

        return 0;
    }
}