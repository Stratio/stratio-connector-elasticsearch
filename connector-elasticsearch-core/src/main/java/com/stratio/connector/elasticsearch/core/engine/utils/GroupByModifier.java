/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */
package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.connector.meta.ColumnGroupBy;
import com.stratio.connector.meta.GroupBy;
import com.stratio.meta.common.statements.structures.selectors.GroupByFunction;
import org.elasticsearch.action.search.SearchRequestBuilder;

import java.util.List;


/**
 * @author darroyo
 */
public class GroupByModifier {

    private GroupByModifier() {
    }

    public static void modify(SearchRequestBuilder requestBuilder, GroupBy groupBy) {

        List<String> fieldsGroup = groupBy.getFieldsGroup();

        for (ColumnGroupBy columns : groupBy.getColumns()) {
            String field = columns.getIdentifiers();
            GroupByFunction groupFunction = columns.getGroupByFunction();
            switch (groupFunction) {
                case SUM:
                    //AggregationBuilders.filter("").subAggregation(aggregation)
                    //AggregationBuilders.global("").
                    //AggregationBuilders.nested(name)
                    //AggregationBuilders.terms(name)
                    //SUBAGGREGATIONS=>http://elasticsearch-users.115913.n3.nabble.com/Java-API-for-multiple-sub-aggregations-td4055253.html
                    //requestBuilder.addAggregation(AggregationBuilders.sum(columns.getAlias()).field(field).);
                    //ValuesSourceAggregationBuilder<ValuesSourceAggregationBuilder<B>>
                    break;


                case MIN:
                    break;
                case MAX:
                    break;
                case AVG:
                    break;
                case COUNT:
                    break;
                default:
                    break;
            }

        }


    }
}
