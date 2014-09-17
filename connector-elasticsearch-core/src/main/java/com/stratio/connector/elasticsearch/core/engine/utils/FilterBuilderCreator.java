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


import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Collection;


public class FilterBuilderCreator {



    private SelectorHelper selectorHelper = new SelectorHelper();

    public  FilterBuilder createFilterBuilder(Collection<Filter> filters) throws UnsupportedException {


        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
          for (Filter filter : filters) {
            Relation relation = filter.getRelation();
            boolFilterBuilder.must(handleCompareFilter(relation));
        }

        return boolFilterBuilder;

    }




    private  FilterBuilder handleCompareFilter(Relation relation) throws UnsupportedException {

        FilterBuilder localFilterBuilder = null;
        // TermFilter: Filters documents that have fields that contain a
        // term (not analyzed)

        String leftTerm = selectorHelper.getSelectorField(relation.getLeftTerm());
        String rightTerm = selectorHelper.getSelectorField(relation.getRightTerm());
        switch (relation.getOperator()) {
            case COMPARE:
            case ASSIGN:
                localFilterBuilder = FilterBuilders.termFilter(leftTerm, rightTerm);
                break;
            case DISTINCT:
                localFilterBuilder = FilterBuilders.notFilter(FilterBuilders.termFilter(leftTerm, rightTerm));
                break;
            case LT:
                localFilterBuilder = FilterBuilders.rangeFilter(leftTerm).lt(rightTerm);
                break;
            case LET:
                localFilterBuilder = FilterBuilders.rangeFilter(leftTerm).lte(rightTerm);
                break;
            case GT:
                localFilterBuilder = FilterBuilders.rangeFilter(leftTerm).gt(rightTerm);
                break;
            case GET:
                localFilterBuilder = FilterBuilders.rangeFilter(leftTerm).gte(rightTerm);
                break;

            default:
                throw new UnsupportedException("Not implemented yet in filter query. [" + relation.getOperator() + "]");

        }


        return localFilterBuilder;

    }







}
