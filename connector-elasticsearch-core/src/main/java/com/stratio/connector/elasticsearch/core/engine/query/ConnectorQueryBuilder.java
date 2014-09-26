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
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryBuilder {

    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

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
        limitModifier.modify(requestBuilder,  queryData);

    }


    private void createProjection(ConnectorQueryData queryData) {
        ProjectCreator projectModifier = new ProjectCreator();
        projectModifier.modify(requestBuilder, queryData.getProjection());
    }

    private void createFilter(ConnectorQueryData queryData) throws UnsupportedException {

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
