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

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.elasticsearch.core.engine.utils.*;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.OrderDirection;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryBuilder {

    /**
     * The log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The request builder.
     */
    private SearchRequestBuilder requestBuilder;

    /**
     * @param elasticClient the elasticSearch Client.
     * @param queryData     the query representation,
     * @return The searchBuilder.
     * @throws UnsupportedException if the operation is not supported.
     * @throws ExecutionException   if the method fails during execution.
     */
    public ActionRequestBuilder buildQuery(Client elasticClient, ProjectParsed queryData) throws UnsupportedException,
            ExecutionException {

        createRequestBuilder(elasticClient);
        createFilter(queryData);
        createProjection(queryData);
        createSelect(queryData);
        createSort(queryData);
        createLimit(queryData); //TODO https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html
        createAgreggation(queryData);

        logQuery();

        return requestBuilder;
    }

    private void createAgreggation(ProjectParsed queryData) throws ExecutionException {

        if (isAgregation(queryData)) {

            if (queryData.getGroupBy().getIds().size() > 1) {
                String message = "Multiple Column GroupBy isn't supported in this Datastore";
                logger.error(message);
                throw new ExecutionException(message);
            }

            Selector selector = queryData.getGroupBy().getIds().iterator().next();
            AggregationBuilder aggregationBuilder = AggregationBuilders.terms(selector.getColumnName().getName()).field(selector.getColumnName().getName()).minDocCount(1).order(Terms.Order.count(false));
            requestBuilder.addAggregation(aggregationBuilder);
            requestBuilder.setSize(0);
        }
    }

    /**
     * This method create the Sort using the OrderBy clause
     *
     * @param queryData
     */
    private void createSort(ProjectParsed queryData) {

        if (null != queryData.getOrderBy() && !queryData.getOrderBy().getIds().isEmpty()) {
            // For each sort
            for (OrderByClause orderBy : queryData.getOrderBy().getIds()) {
                boolean ascendingWay = orderBy.getDirection().equals(OrderDirection.ASC);
                String fieldName = orderBy.getSelector().getColumnName().getName();
                SortBuilder sortBuilder = SortBuilders.fieldSort(fieldName);
                sortBuilder.order(ascendingWay ? SortOrder.ASC : SortOrder.DESC);
                this.requestBuilder.addSort(sortBuilder);
            }
        }
    }

    /**
     * This method create the select part of the query.
     *
     * @param queryData the querydata.
     */
    private void createSelect(ProjectParsed queryData) {
        SelectCreator selectCreator = new SelectCreator();
        if (!isAgregation(queryData)) {
            selectCreator.modify(requestBuilder, queryData.getSelect());
        }

    }

    /**
     * This method crete the elasticsearch request builder.
     *
     * @param elasticClient the elasticsearch client..
     */
    private void createRequestBuilder(Client elasticClient) {
        requestBuilder = elasticClient.prepareSearch();

    }

    /**
     * Logger the query.
     */
    private void logQuery() {
        if (logger.isDebugEnabled()) {
            logger.debug("ElasticSearch Query: [" + requestBuilder + "]");
        }
    }

    /**
     * This method crete the Limit part of the query.
     */
    private void createLimit(ProjectParsed queryData) throws ExecutionException {
        // LimitModifier limitModifier = new LimitModifier();
        //limitModifier.modify(requestBuilder);
        if (queryData.getLimit() != null) {
            requestBuilder.setSize(queryData.getLimit().getLimit());
        }
    }

    /**
     * This method crete the Limit part of the query.
     *
     * @param queryData the querydata.
     */
    private void createProjection(ProjectParsed queryData) {
        ProjectCreator projectModifier = new ProjectCreator();
        projectModifier.modify(requestBuilder, queryData.getProject());
    }

    /**
     * This method crete the Filter part of the query.
     *
     * @param queryData the querydata.
     */
    private void createFilter(ProjectParsed queryData) throws UnsupportedException, ExecutionException {

        QueryBuilderFactory queryBuilderFactory = new QueryBuilderFactory();
        QueryBuilder queryBuilder = queryBuilderFactory.createBuilder(queryData.getMatchList(), queryData.getFunctionFilters());

        if (!queryData.getFilter().isEmpty()) {
            FilterBuilderCreator filterBuilderCreator = new FilterBuilderCreator();
            FilterBuilder filterBuilder = filterBuilderCreator.createFilterBuilder(queryData.getFilter());
            requestBuilder.setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder));
        } else {
            requestBuilder.setQuery(queryBuilder);

        }
    }

    private boolean isAgregation(ProjectParsed queryData) {
        return queryData.getGroupBy() != null && !queryData.getGroupBy().getIds().isEmpty();
    }
}
