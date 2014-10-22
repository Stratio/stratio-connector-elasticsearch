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

package com.stratio.connector.elasticsearch.core.engine.query;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.elasticsearch.core.engine.utils.FilterBuilderCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier;
import com.stratio.connector.elasticsearch.core.engine.utils.ProjectCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.QueryBuilderCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.SelectCreator;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryBuilder {

    /**
     * The log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private SearchRequestBuilder requestBuilder;

    /**
     * @param elasticClient the elasticSearch Client.
     * @param queryData     the query representation,
     * @return The searchBuilder.
     * @throws UnsupportedException if the operation is not supported.
     * @throws ExecutionException   if the method fails during execution.
     */
    public SearchRequestBuilder buildQuery(Client elasticClient, ConnectorQueryData queryData)
            throws UnsupportedException, ExecutionException {

        createRequestBuilder(elasticClient);
        createFilter(queryData);
        createProjection(queryData);
        createLimit(queryData);
        createSelect(queryData);

        logQuery();

        return requestBuilder;
    }

    private void createSelect(ConnectorQueryData queryData) {
        if (queryData.getSelect() != null && queryData.getSelect().getColumnMap() != null) {
            SelectCreator selectCreator = new SelectCreator();

            selectCreator.modify(requestBuilder, queryData.getSelect());
        }
    }

    private void createRequestBuilder(Client elasticClient) {
        requestBuilder = elasticClient.prepareSearch();
    }

    private void logQuery() {
        if (logger.isDebugEnabled()) {
            logger.debug("ElasticSearch Query: [" + requestBuilder + "]");
        }
    }

    private void createLimit(ConnectorQueryData queryData) throws ExecutionException {
        LimitModifier limitModifier = new LimitModifier();
        limitModifier.modify(requestBuilder, queryData);

    }

    private void createProjection(ConnectorQueryData queryData) {
        ProjectCreator projectModifier = new ProjectCreator();
        projectModifier.modify(requestBuilder, queryData.getProjection());
    }

    private void createFilter(ConnectorQueryData queryData) throws UnsupportedException, ExecutionException {

        QueryBuilderCreator queryBuilderCreator = new QueryBuilderCreator();
        QueryBuilder queryBuilder = queryBuilderCreator.createBuilder(queryData.getMatchList());

        if (queryData.hasFilterList()) {
            FilterBuilderCreator filterBuilderCreator = new FilterBuilderCreator();
            FilterBuilder filterBuilder = filterBuilderCreator.createFilterBuilder(queryData.getFilter());
            requestBuilder.setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder));
        } else {
            requestBuilder.setQuery(queryBuilder);

        }
    }

}
