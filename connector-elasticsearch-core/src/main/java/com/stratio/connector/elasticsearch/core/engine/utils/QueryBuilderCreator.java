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

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.statements.structures.relationships.Relation;

/**
 * Created by jmgomez on 17/09/14.
 */
public class QueryBuilderCreator {



    public QueryBuilder createBuilder(Collection<Filter> matchList) throws ExecutionException {

        QueryBuilder queryBuilder;

        if (matchList.isEmpty()) {
            queryBuilder = QueryBuilders.matchAllQuery();
        } else {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (Filter filter : matchList) {
                Relation relation = filter.getRelation();
                String leftTerm = SelectorHelper.getValue(String.class, relation.getLeftTerm());
                String rightTerm =  SelectorHelper.getValue(String.class, relation.getRightTerm());

                boolQueryBuilder.must(QueryBuilders.matchQuery(leftTerm, rightTerm));

            }

            queryBuilder = boolQueryBuilder;
        }

        return queryBuilder;
    }
}
