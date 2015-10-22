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

import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.elasticsearch.core.engine.query.functions.ESFunction;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Disjunction;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.FunctionFilter;
import com.stratio.crossdata.common.logicalplan.ITerm;
import com.stratio.crossdata.common.statements.structures.AbstractRelation;
import com.stratio.crossdata.common.statements.structures.Relation;
import org.elasticsearch.index.query.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The responsibility of this class is create a QueryBuilder.
 * Created by jmgomez on 17/09/14.
 */
public class QueryBuilderFactory {



    public QueryBuilder createBuilder(Collection<Filter> filters) throws ExecutionException, UnsupportedException {
        return createBuilder(filters, new ArrayList<FunctionFilter>(), new ArrayList<Disjunction>());
    }

    /**
     * Create a query builder.
     *
     * @param filters Match and rangeQuery Functions
     * @return a queryBuilder. the queryBuilder.
     * @throws ExecutionException   if any error happens.
     * @throws UnsupportedException if the operation is not supported.
     */
    public QueryBuilder createBuilder(Collection<Filter> filters, Collection<FunctionFilter> functionFilters, Collection<Disjunction> disjunctions) throws ExecutionException, UnsupportedException {

        QueryBuilder queryBuilder;

        if (filters.isEmpty() && functionFilters.isEmpty() && disjunctions.isEmpty()) {
            queryBuilder = QueryBuilders.matchAllQuery();
        } else {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery(); //"bool" : {

            for(FunctionFilter functionFilter: functionFilters){
                boolQueryBuilder.must(ESFunction.build(functionFilter.getRelation()).buildQuery());
            }

            for (Filter filter : filters) {
                boolQueryBuilder.must(createQueryBuilder(filter.getRelation())); // "must" : {
            }

            if (!disjunctions.isEmpty()) {
                boolQueryBuilder.must(createFilterBuilderForDisjunctions(disjunctions));
            }

            queryBuilder = boolQueryBuilder;
        }

        return queryBuilder;
    }

    /**
     * Turn a relation into a queryBuilder.
     *
     * @param abstractRelation the relation.
     * @return the queryBuilder.
     * @throws ExecutionException   if any error happens.
     * @throws UnsupportedException if the operation is not supported.
     */
    private QueryBuilder createQueryBuilder(AbstractRelation abstractRelation)
            throws ExecutionException, UnsupportedException {

        QueryBuilder queryBuilderfilter;


            Relation relation = (Relation) abstractRelation;
            String leftTerm = SelectorHelper.getValue(String.class, relation.getLeftTerm());
            String rightTerm = SelectorHelper.getValue(String.class, relation.getRightTerm());

            switch (relation.getOperator()) {
                case MATCH:
                case EQ:
                    queryBuilderfilter = QueryBuilders.matchQuery(leftTerm, rightTerm.toLowerCase());
                    break;
                case LT:
                    queryBuilderfilter = QueryBuilders.rangeQuery(leftTerm).lt(rightTerm.toLowerCase());
                    break;
                case LET:
                    queryBuilderfilter = QueryBuilders.rangeQuery(leftTerm).lte(rightTerm.toLowerCase());
                    break;
                case GT:
                    queryBuilderfilter = QueryBuilders.rangeQuery(leftTerm).gt(rightTerm.toLowerCase());
                    break;
                case GET:
                    queryBuilderfilter = QueryBuilders.rangeQuery(leftTerm).gte(rightTerm.toLowerCase());
                    break;

                default:
                    throw new UnsupportedException("The operation [" + relation.getOperator() + "] is not supported");


        }
        return queryBuilderfilter;
    }


    /**
     * Creates a filterBuilder for Disjunctions (OR)
     * @param disjunctions a List of OR
     * @return the FilterBuilder with the logical tree.
     * @throws UnsupportedException
     * @throws ExecutionException
     */
    /*
     * Magic! Touch only if you know what are you doing!
     */
    public QueryBuilder createFilterBuilderForDisjunctions(Collection<Disjunction> disjunctions) throws UnsupportedException,
            ExecutionException {

        // Where (x=1 or x=2) AND (y=1 OR y=2), Multiple disjunctions
        if (disjunctions.size() > 1) {
            List<BoolQueryBuilder> external = new ArrayList<>();
            for (Disjunction disjunction : disjunctions) {
                List<QueryBuilder> internal = new ArrayList<>();
                for (List<ITerm> terms : disjunction.getTerms()) {
                    List<QueryBuilder> internal2 = new ArrayList<>();
                    Collection<Disjunction> internalDisjunction = new ArrayList<>();
                    for (ITerm term : terms) {
                        if (term instanceof FunctionFilter) {
                            internal2.add(ESFunction.build(((FunctionFilter) term).getRelation()).buildQuery());
                        } else if (term instanceof Disjunction) {
                            internalDisjunction.add((Disjunction) term);
                        }
                    }
                    internal.addAll(internal2);
                    if (!internalDisjunction.isEmpty()) {
                        internal.add(createFilterBuilderForDisjunctions(internalDisjunction));
                    }
                }

                for(QueryBuilder queryBuilder: internal ){
                    external.add(QueryBuilders.boolQuery().should(queryBuilder));
                }

            }
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

            for (BoolQueryBuilder boolQueryBuilder1:external){
                boolQueryBuilder.must(boolQueryBuilder1);
            }

            return boolQueryBuilder;
        } else {// Where (x=1 and x=2) OR (y=1 AND y=2) Disjunctions in a Tree
            Disjunction disjunction = disjunctions.iterator().next();
            List<QueryBuilder> internal = new ArrayList<>();
            for (List<ITerm> terms : disjunction.getTerms()) {
                List<QueryBuilder> internal2 = new ArrayList<>();
                Collection<Disjunction> internalDisjunction = new ArrayList<>();
                for (ITerm term : terms) {
                    if (term instanceof FunctionFilter) {
                        internal2.add(ESFunction.build(((FunctionFilter) term).getRelation()).buildQuery());
                    } else if (term instanceof Disjunction) {
                        internalDisjunction.add((Disjunction) term);
                    }
                }

                for (QueryBuilder queryBuilder :internal2){
                    internal.add(QueryBuilders.boolQuery().must(queryBuilder));
                }

                if (!internalDisjunction.isEmpty()) {
                    internal.add(createFilterBuilderForDisjunctions(internalDisjunction));
                }
            }
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for(QueryBuilder queryBuilder:internal){
                boolQueryBuilder.should(queryBuilder);
            }

            return boolQueryBuilder;
        }
    }
}
