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
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.statements.structures.relationships.Relation;

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
                String rightTerm = SelectorHelper.getValue(String.class, relation.getRightTerm());

                boolQueryBuilder.must(QueryBuilders.matchQuery(leftTerm, rightTerm.toLowerCase()));

            }

            queryBuilder = boolQueryBuilder;
        }

        return queryBuilder;
    }
}
