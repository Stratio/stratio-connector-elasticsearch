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

import com.stratio.connector.commons.util.FilterHelper;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Disjunction;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.ITerm;
import com.stratio.crossdata.common.statements.structures.*;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    public FilterBuilder createFilterBuilderForDisjunctions(Collection<Disjunction> disjunctions) throws UnsupportedException,
            ExecutionException {

        if (disjunctions.size() > 1) {
            List<BoolFilterBuilder> external = new ArrayList<>();
            for (Disjunction disjunction : disjunctions) {
                List<FilterBuilder> internal = new ArrayList<>();
                for (List<ITerm> terms : disjunction.getTerms()) {
                    List<FilterBuilder> internal2 = new ArrayList<>();
                    Collection<Disjunction> internalDisjunction = new ArrayList<>();
                    for (ITerm term : terms) {
                        if (term instanceof Filter) {
                            internal2.add(handleCompareFilter((Filter) term));
                        } else if (term instanceof Disjunction) {
                            internalDisjunction.add((Disjunction) term);
                        }
                    }
                    internal.addAll(internal2);
                    if (!internalDisjunction.isEmpty()) {
                        internal.add(createFilterBuilderForDisjunctions(internalDisjunction));
                    }
                }
                external.add(FilterBuilders.boolFilter().should(internal.toArray(new FilterBuilder[]{})));
            }
            BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
            boolFilterBuilder.must(external.toArray(new BoolFilterBuilder[]{}));
            return boolFilterBuilder;
        } else {
            BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
            Disjunction disjunction = disjunctions.iterator().next();
            List<FilterBuilder> internal = new ArrayList<>();
            for (List<ITerm> terms : disjunction.getTerms()) {
                List<FilterBuilder> internal2 = new ArrayList<>();
                Collection<Disjunction> internalDisjunction = new ArrayList<>();
                for (ITerm term : terms) {
                    if (term instanceof Filter) {
                        internal2.add(handleCompareFilter((Filter) term));
                    } else if (term instanceof Disjunction) {
                        internalDisjunction.add((Disjunction) term);
                    }
                }
                internal.add(FilterBuilders.boolFilter().must(internal2.toArray(new FilterBuilder[]{})));
                if (!internalDisjunction.isEmpty()) {
                    internal.add(createFilterBuilderForDisjunctions(internalDisjunction));
                }
            }
            boolFilterBuilder.should(internal.toArray(new FilterBuilder[]{}));
            return boolFilterBuilder;
        }
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

        Object rightTerm = getSelectorValue(relation.getRightTerm());

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
                GroupSelector selector = (GroupSelector) filter.getRelation().getRightTerm();
                Object from = SelectorHelper.getValue(SelectorHelper.getClass(selector.getFirstValue()),
                        selector.getFirstValue());
                Object to = SelectorHelper.getValue(SelectorHelper.getClass(selector.getLastValue()),
                        selector.getLastValue());

                localFilterBuilder = FilterBuilders.rangeFilter(leftTerm).from(from).to(to);
                break;
            case IN:
            case NOT_IN:
                ListSelector inSelector = (ListSelector) filter.getRelation().getRightTerm();
                FilterBuilder[] values = new FilterBuilder[inSelector.getSelectorsList().size()];

                for (int i = 0; i < values.length; i++) {
                    values[i] = FilterBuilders.termFilter(leftTerm, getSelectorValue(inSelector.getSelectorsList().get(i)));

                }
                if (relation.getOperator().equals(Operator.IN)) {
                    localFilterBuilder = FilterBuilders.boolFilter().should(values);
                } else {
                    localFilterBuilder = FilterBuilders.notFilter(FilterBuilders.boolFilter().should(values));
                }

                break;
            default:
                throw new UnsupportedException("Not implemented yet in filter query. [" + relation.getOperator() + "]");

        }

        return localFilterBuilder;

    }

    private Object getSelectorValue(Selector rightValue) throws ExecutionException {
        Object rightTerm = SelectorHelper.getValue(SelectorHelper.getClass(rightValue),
                rightValue);
        if (rightTerm instanceof String) {
            rightTerm = ((String) rightTerm).toLowerCase();
        }
        return rightTerm;
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
