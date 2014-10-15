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
import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Relation;

public class FilterBuilderCreator {

    public FilterBuilder createFilterBuilder(Collection<Filter> filters) throws UnsupportedException,
            ExecutionException {

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
        for (Filter filter : filters) {

            boolFilterBuilder.must(handleCompareFilter(filter));
        }

        return boolFilterBuilder;

    }

    private FilterBuilder handleCompareFilter(Filter filter) throws UnsupportedException, ExecutionException {
        Relation relation = filter.getRelation();

        FilterBuilder localFilterBuilder = null;
        // TermFilter: Filters documents that have fields that contain a
        // term (not analyzed)

        String leftTerm = recoveredLeftTerm(filter, relation);
        Object rightTerm = SelectorHelper.getValue(SelectorHelper.getClass(relation.getRightTerm()),
                relation.getRightTerm());
        if (rightTerm instanceof String) {
            rightTerm = ((String) rightTerm).toLowerCase();
        }
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

    private String recoveredLeftTerm(Filter filter, Relation relation)
            throws ExecutionException {
        String leftTerm = SelectorHelper.getValue(String.class, relation.getLeftTerm());
        if (isPK(filter)) {
            leftTerm = "_id";
        }
        return leftTerm;
    }

    private boolean isPK(Filter filter) {
        return filter.getOperation().equals(Operations.FILTER_PK_DISTINCT) || filter.getOperation().equals(Operations
                .FILTER_PK_EQ) || filter.getOperation().equals(Operations.FILTER_PK_GET) || filter.getOperation()
                .equals(Operations.FILTER_PK_GT) || filter.getOperation().equals(Operations.FILTER_PK_LET) || filter
                .getOperation().equals(Operations.FILTER_PK_LT);
    }

}
