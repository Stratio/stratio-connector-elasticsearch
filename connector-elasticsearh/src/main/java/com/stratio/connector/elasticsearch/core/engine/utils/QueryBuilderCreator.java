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

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.Relation;

/**
 * The responsibility of this class is create a QueryBuilder.
 * Created by jmgomez on 17/09/14.
 */
public class QueryBuilderCreator {

    /**
     * Create a query builder.
     *
     * @param filters the filters.
     * @return a queryBuilder. the queryBuidler.
     * @throws ExecutionException   if any error happens.
     * @throws UnsupportedException if the operation is not supported.
     */
    public QueryBuilder createBuilder(Collection<Filter> filters) throws ExecutionException, UnsupportedException {

        QueryBuilder queryBuilder;

        if (filters.isEmpty()) {
            queryBuilder = QueryBuilders.matchAllQuery();
        } else {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (Filter filter : filters) {
                boolQueryBuilder.must(createQueryBuilder(filter.getRelation()));
            }

            queryBuilder = boolQueryBuilder;
        }

        return queryBuilder;
    }

    /**
     * Turn a relation into a queryBuilder.
     *
     * @param relation the relation.
     * @return the queryBuilder.
     * @throws ExecutionException   if any error happens.
     * @throws UnsupportedException if the operation is not supported.
     */
    private QueryBuilder createQueryBuilder(Relation relation)
            throws ExecutionException, UnsupportedException {

        QueryBuilder queryBuilderfilter;

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
}
