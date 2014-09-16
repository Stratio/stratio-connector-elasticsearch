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
package com.stratio.connector.elasticsearch.core.engine;

import com.stratio.connector.elasticsearch.core.engine.utils.FilterBuilderHelper;
import com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier;
import com.stratio.connector.elasticsearch.core.engine.utils.ProjectModifier;
import com.stratio.connector.elasticsearch.core.engine.utils.SortModifier;
import com.stratio.connector.elasticsearch.core.exceptions.ElasticsearchQueryException;
import com.stratio.connector.meta.ElasticsearchResultSet;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.Sort;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author darroyo
 */
public class LogicalPlanExecutor {

    final Logger logger = LoggerFactory.getLogger(this.getClass());


    private SearchType searchType = null;

    private Project projection = null;
    private ArrayList<Sort> sortList = null;
    private Limit limitValue = null;
    private ArrayList<Filter> filterList = null;
    //	private GroupBy groupBy = null;
//	private ArrayList<Match> matchList = null; //REVIEW a la espera de meta
    private SearchRequestBuilder requestBuilder = null;


    public void readLogicalPlan(LogicalWorkflow logicalWorkFlow) throws ElasticsearchQueryException, UnsupportedOperationException {

        List<LogicalStep> logicalSteps = logicalWorkFlow.getInitialSteps();
        sortList = new ArrayList<Sort>();
        filterList = new ArrayList<Filter>();


        for (LogicalStep lStep : logicalSteps) {

            if (lStep instanceof Project) {
                if (projection == null) projection = (Project) lStep;
                else throw new UnsupportedOperationException(" # Project > 1");
            } else if (lStep instanceof Sort) {
                sortList.add((Sort) lStep);
            } else if (lStep instanceof Limit) {
                if (limitValue == null) limitValue = (Limit) lStep;
                else throw new UnsupportedOperationException(" # Limit > 1");
            } else if (lStep instanceof Filter) {
                filterList.add((Filter) lStep);
            } else
                throw new UnsupportedOperationException("LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported");

        }

        checkSupportedQuery();

    }

    private void checkSupportedQuery() {
        if (projection == null) throw new UnsupportedOperationException("no projection found", null);
        if (!sortList.isEmpty() && limitValue == null)
            throw new UnsupportedOperationException("cannot sort: limit is required");
    }


    public void buildQuery(Client elasticClient) throws UnsupportedException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {
        //TODO Multisearch? elasticClient.prepareMultiSearch().add(requestBuilder);
        //TODO if count or distinct => prepare Count?? or aggregation
        requestBuilder = elasticClient.prepareSearch();


        //SET QUERY
        //	QueryBuilder queryBuilder = MatchBuilderHelper.createMatchBuilder(matchList); //REVIEW a la espera de meta
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();  //REVIEW a la esperea de meta
        FilterBuilder filterBuilder = FilterBuilderHelper.createFilterBuilder(filterList);

        if (filterBuilder != null) {
            requestBuilder.setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder));
        } else requestBuilder.setQuery(queryBuilder); //REVIEW a la espera de meta


        //MODIFY QUERY
        ProjectModifier.modify(requestBuilder, projection);
        LimitModifier.modify(requestBuilder, limitValue, searchType);
        if (!sortList.isEmpty()) SortModifier.modify(requestBuilder, sortList);

        //	if (groupBy != null) GroupByModifier.modify(requestBuilder, groupBy);  //REVIEW a la espera de meta

        if (logger.isDebugEnabled()) {
            logger.debug("ElasticSearch Query: [" + requestBuilder + "]");
        }

    }


    /**
     *
     */
    public void setSearchType() {
        if (limitValue == null) searchType = SearchType.SCAN;//TODO or limit < XX
        else {
            /*//REVIEW
             * if(!matchList.isEmpty()) searchType = SearchType.DFS_QUERY_THEN_FETCH; //TODO check if query is constant filtered?
			else //TODO IF SORT IS EMPTY=> SCAN o QUERY_THEN_FETCH o setSort(Empty)(default=>score?)
			*/
            searchType = (sortList.isEmpty()) ? SearchType.QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH;


        }
    }


    /**
     * Queries for objects in a collection
     */

    public QueryResult executeQuery(Client elasticClient) {

        //TODO
        QueryResult queryResult = null;
        try {

            ElasticsearchResultSet resultSet = new ElasticsearchResultSet();
            //resultSet.setColumnMetadata(projection.getColumnList());// needed?? //REVIEW comentado por que este metodo ya no existe.
            SearchResponse scrollResp;


            if (searchType == SearchType.QUERY_THEN_FETCH || searchType == SearchType.DFS_QUERY_THEN_FETCH) {
                scrollResp = requestBuilder.execute().actionGet();
                for (SearchHit hit : scrollResp.getHits()) {
                    resultSet.add(createRow(hit));
                }
            } else if (searchType == SearchType.SCAN) {
                //Prepare to SCAN
                scrollResp = requestBuilder.execute().actionGet();
                do {
                    scrollResp = elasticClient.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(LimitModifier.SCAN_TIMEOUT_MILLIS)).execute().actionGet();
                    for (SearchHit hit : scrollResp.getHits()) {
                        resultSet.add(createRow(hit));
                    }
                } while (scrollResp.getHits().getHits().length != 0);

            }

//		if(groupBy != null){ //REVIEW a la espera de meta
//			Aggregation agg = scrollResp.getAggregations().get("keys");
//			agg.
//			response.getAggregations().get("keys");
//			Collection<Terms.Bucket> buckets = terms.getBuckets();
            //}

            queryResult = QueryResult.createQueryResult(resultSet);
        } catch (IndexMissingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("The index not exists. The ES connector returns an empty QueryResult");
            }
            queryResult = QueryResult.createQueryResult(new ElasticsearchResultSet());
        }
        return queryResult;
    }


    /**
     * This method creates a row from a mongoResult
     *
     * @param hit the Elasticsearch SearchHit.
     * @return the row.
     */
    private Row createRow(SearchHit hit) {
        Row row = new Row();

        //TODO match?lucene filter?
        /*
         * //check score
		//get fields instead of get source?
		for (Map.Entry<String, Object> entry : hit.getSource().entrySet())	{
				row.addCell(entry.getKey(), new Cell(entry.getValue()));
		}*/

        for (Map.Entry<String, SearchHitField> entry : hit.fields().entrySet()) {
            row.addCell(entry.getKey(), new Cell(entry.getValue().getValue()));
        }

        return row;
    }


}
