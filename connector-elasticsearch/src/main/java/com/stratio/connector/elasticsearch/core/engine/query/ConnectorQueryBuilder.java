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
import com.stratio.connector.elasticsearch.core.engine.utils.FilterBuilderCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.QueryBuilderFactory;
import com.stratio.connector.elasticsearch.core.engine.utils.SelectorUtils;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.*;
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
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Class that creates an elasticsearch query from a SELECT clause
 *
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

        if (null == elasticClient){throw new ExecutionException("Query builder received an empty client to execute the query.");}
        if (null == queryData){throw new ExecutionException("Query builder received an empty select clause to be processed.");}

        createRequestBuilder(elasticClient);
        createProjection(queryData.getProject());
        createFilter(queryData);

        if (isAggregation(queryData)){
            createNestedTermAggregation(queryData.getGroupBy());}
        else {
            createSelect(queryData.getSelect());
            createSort(queryData.getOrderBy());
            createLimit(queryData.getLimit()); //TODO https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-search-type.html
        }

        logQuery();

        return requestBuilder;
    }


    /**
     * Method that creates the elasticsearch request builder.
     *
     * @param elasticClient the elasticsearch client..
     */
    private void createRequestBuilder(Client elasticClient) {
        requestBuilder = elasticClient.prepareSearch();
    }

    /**
     * Method that creates base query request including the index to be searched and the data type
     *
     * @param projection object including the index and data type information
     */
    private void createProjection(Project projection) {
        // Sets the index to be searched
        requestBuilder.setIndices(projection.getCatalogName());
        // Sets the data type
        requestBuilder.setTypes(projection.getTableName().getName());
    }


    /**
     * Method that creates the Filter part of the query.
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


    /**
     * Method that creates the appropriate nested term aggregation properties to elasticsearch based on those specified by the "group by" clause
     *
     * @param groupBy           GROUP BY clause that defines the nested term aggregations to be retrieved
     * @throws ExecutionException
     */
    private void createNestedTermAggregation(GroupBy groupBy) throws ExecutionException {
        // If any "group by" fields are defined they are used in order to create the appropriate aggregations
        if (null != groupBy && null != groupBy.getIds()){
            AggregationBuilder aggregationBuilder = null;

            // First field is used as the parent aggregation level and next ones are added as nested sub-aggregations of the previous one
            for(Selector term: groupBy.getIds()){
                String fieldName = SelectorUtils.getSelectorFieldName(term);
                if (aggregationBuilder == null){
                    aggregationBuilder = AggregationBuilders.terms(fieldName).field(fieldName);
                }else{
                    aggregationBuilder.subAggregation(AggregationBuilders.terms(fieldName).field(fieldName));
                }
            }

            // No results are needed when requesting an aggregation therefore the size is set to 0 and the aggregation properties are added to the query
            requestBuilder.addAggregation(aggregationBuilder);
            requestBuilder.setSize(0);
        }
    }

    /**
     * Method that adds the appropriate returning fields to the elasticsearch query from those specified by the select clause
     *
     * @param select SELECT clause including fields to be returned
     */
    private void createSelect(Select select) {
        // If any "select" fields are requested they are used in order to set the return fields in the elasticsearch query
        if (null != select && null != select.getColumnMap() && !select.getColumnMap().isEmpty()){

            Set<Selector> selectors = select.getColumnMap().keySet();

            // If it is a count operation no field is needed to be returned
            if (isCount(selectors)){return;}

            // Otherwise field names are added to the return fields property
            for (Selector selector : selectors) {
                // Gets the field name associated to the selector
                String fieldName = SelectorUtils.getSelectorFieldName(selector);

                // Adds field name to returning field property in the elasticsearch query
                if (null != fieldName) {requestBuilder.addField(fieldName);}
            }
        }
    }


    /**
     * Method that adds the appropriate sorts to the elasticsearch query from those specified by the OrderBy clause
     *
     * @param orderBy   ORDER BY clause including fields to be used for sorting and sorting ways
     */
    private void createSort(OrderBy orderBy) {
        // Checks if any order by clause is present
        if (null != orderBy && null != orderBy.getIds() && !orderBy.getIds().isEmpty()) {

            // For each clause a new sort property is added to the query
            for (OrderByClause orderByClause : orderBy.getIds()) {

                // Retrieves selector (field or field function) which is going to be used to sort results and the sort way (ascending or descending)
                Selector selector = orderByClause.getSelector();
                boolean ascendingWay = OrderDirection.ASC.equals(orderByClause.getDirection());

                // Gets the field name associated to the selector
                String fieldName = SelectorUtils.getSelectorFieldName(selector);

                // Creates the appropriate sort property and adds it to the query builder
                if (null != fieldName) {
                    SortBuilder sortBuilder = SortBuilders.fieldSort(fieldName);
                    sortBuilder.order(ascendingWay ? SortOrder.ASC : SortOrder.DESC);
                    this.requestBuilder.addSort(sortBuilder);
                }
            }
        }
    }


    /**
     * Method that adds the size property to the elasticsearch query based on the limit property from the original clause.
     *
     * @param limit LIMIT clause that defines the number of results to be returned
     */
    private void createLimit(Limit limit) throws ExecutionException {
        if (null != limit) {requestBuilder.setSize(limit.getLimit());}
    }



    // UTILITY METHODS

    /**
     * Checks whether a select clause is an aggregation operation by checking if includes a "group by" clause
     *
     * @param queryData   complete information from the SELECT clause
     * @return                      true if the clause is a count operation or false otherwise
     */
    private boolean isAggregation(ProjectParsed queryData) {
        return queryData.getGroupBy() != null && !queryData.getGroupBy().getIds().isEmpty();
    }


    /**
     * Checks whether a select clause is a count operation by checking if includes the function "count(...)"
     *
     * @param selectors     list of fields from the SELECT clause
     * @return                      true if the clause is a count operation or false otherwise
     */
    private boolean isCount (Set<Selector> selectors){
        // If any of the selectors is a count function the the clause is a count operation
        for (Selector selector : selectors) {
            if (SelectorUtils.isFunction(selector, "count")){return true;}
        }
        return false;
    }


    /**
     * Logger the query.
     */
    private void logQuery() {
        if (logger.isDebugEnabled()) {
            logger.debug("ElasticSearch Query: [" + requestBuilder + "]");
        }
    }
}