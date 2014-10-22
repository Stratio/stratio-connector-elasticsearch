/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.elasticsearch.core.engine.utils;

import java.util.Collection;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.connector.Operations;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.relationships.Relation;

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
