/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.engine.utils;

import java.util.Collection;

import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.GroupSelector;
import com.stratio.crossdata.common.statements.structures.StringSelector;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.stratio.connector.commons.util.FilterHelper;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * The responsibility of this class is create a FilterBuilder.
 */
public class FilterBuilderCreator {

    /**
     * This method creates a filter builder.
     *
     * @param filters the filters.
     * @return a filter builder.
     * @throws UnsupportedException if a filter type is not supported.
     * @throws ExecutionException   if an error happens.
     */
    public FilterBuilder createFilterBuilder(Collection<Filter> filters) throws UnsupportedException,
            ExecutionException {


        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
        for (Filter filter : filters) {

            boolFilterBuilder.must(handleCompareFilter(filter));
        }

        return boolFilterBuilder;

    }

    /**
     * this method create compara filter.
     *
     * @param filter the filter.
     * @return a filter builder.
     * @throws UnsupportedException if a filter type is not supported.
     * @throws ExecutionException   if an error happens.
     */
    private FilterBuilder handleCompareFilter(Filter filter) throws UnsupportedException, ExecutionException {

        Relation relation = filter.getRelation();
        FilterBuilder localFilterBuilder = null;
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
            case NOT_EQ:
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
            case BETWEEN:
                GroupSelector selector = (GroupSelector)filter.getRelation().getRightTerm();
                Object from = SelectorHelper.getValue(SelectorHelper.getClass(selector.getFirstValue()),
                        selector.getFirstValue());
                Object to = SelectorHelper.getValue(SelectorHelper.getClass(selector.getLastValue()),
                        selector.getLastValue());

                localFilterBuilder = FilterBuilders.rangeFilter(leftTerm).from(from).to(to);
                break;
            default:
                throw new UnsupportedException("Not implemented yet in filter query. [" + relation.getOperator() + "]");

        }

        return localFilterBuilder;

    }

    /**
     * This method recovered the left term.
     *
     * @param filter   the filter.
     * @param relation the relation.
     * @return the left term.
     * @throws ExecutionException if an error happens.
     */
    private String recoveredLeftTerm(Filter filter, Relation relation)
            throws ExecutionException {
        String leftTerm = SelectorHelper.getValue(String.class, relation.getLeftTerm());
        if (FilterHelper.isPK(filter)) {
            leftTerm = "_id";
        }
        return leftTerm;
    }

}
