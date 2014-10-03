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

import java.util.Collection;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Relation;

public class FilterBuilderCreator {



    public FilterBuilder createFilterBuilder(Collection<Filter> filters) throws UnsupportedException,
            ExecutionException {

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
        for (Filter filter : filters) {
            Relation relation = filter.getRelation();
            boolFilterBuilder.must(handleCompareFilter(relation));
        }

        return boolFilterBuilder;

    }

    private FilterBuilder handleCompareFilter(Relation relation) throws UnsupportedException, ExecutionException {

        FilterBuilder localFilterBuilder = null;
        // TermFilter: Filters documents that have fields that contain a
        // term (not analyzed)

        String leftTerm = SelectorHelper.getValue(String.class, relation.getLeftTerm());
        String rightTerm = SelectorHelper.getValue(String.class, relation.getRightTerm());
        switch (relation.getOperator()) {
        case EQ:
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
