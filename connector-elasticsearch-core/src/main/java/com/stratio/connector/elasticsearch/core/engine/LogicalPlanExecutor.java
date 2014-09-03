/**
* Copyright (C) 2014 Stratio (http://stratio.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.stratio.connector.elasticsearch.core.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stratio.meta.common.logicalplan.*;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.elasticsearch.core.engine.utils.FilterBuilderHelper;
import com.stratio.connector.elasticsearch.core.engine.utils.GroupByModifier;
import com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier;
import com.stratio.connector.elasticsearch.core.engine.utils.MatchBuilderHelper;
import com.stratio.connector.elasticsearch.core.engine.utils.ProjectModifier;
import com.stratio.connector.elasticsearch.core.engine.utils.SortModifier;
import com.stratio.connector.elasticsearch.core.exceptions.ElasticsearchQueryException;
import com.stratio.connector.meta.ElasticsearchResultSet;
import com.stratio.connector.meta.GroupBy;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.Match;
import com.stratio.connector.meta.Sort;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;


/**
 * @author darroyo
 *
 */
public class LogicalPlanExecutor {
	

	final Logger logger = LoggerFactory.getLogger(this.getClass());

	

	
	private SearchType searchType = null;
	
	private Project projection = null;
	private ArrayList<Sort> sortList = null;
	private Limit limitValue = null;
	private ArrayList<Filter> filterList = null;
	private GroupBy groupBy = null;
	private ArrayList<Match> matchList = null;	
	private SearchRequestBuilder requestBuilder = null;
	
 /**
 * Construct a new logical plan executor ready to execute the query
 * @param logicalPlan the logical plan to build the query
 * @param elasticClient the Elasticsearch client
 * @throws ElasticsearchQueryException if any trouble happen executing the query
 * @throws com.stratio.connector.meta.exception.UnsupportedOperationException if any operation is not allowed.
 * @throws UnsupportedException
 */
public LogicalPlanExecutor(LogicalWorkflow logicalPlan, Client elasticClient) throws ElasticsearchQueryException, com.stratio.connector.meta.exception.UnsupportedOperationException, UnsupportedException{

		
		readLogicalPlan(logicalPlan);
		setSearchType();
		buildQuery(elasticClient);
}





	private void readLogicalPlan(LogicalWorkflow logicalPlan) throws ElasticsearchQueryException, com.stratio.connector.meta.exception.UnsupportedOperationException {

		List<LogicalStep> logicalSteps = logicalPlan.getInitialSteps(); //REVIEW cambiado por cambio de interfaz
		sortList = new ArrayList<Sort>();
		filterList = new ArrayList<Filter>();
		matchList = new ArrayList<Match>();
		
		for (LogicalStep lStep : logicalSteps) { //TODO validate??
			if (lStep instanceof Project) {
				if (projection == null) projection = (Project) lStep;
				else throw new ElasticsearchQueryException(" # Project > 1");
			} else if (lStep instanceof Sort) {
				sortList.add((Sort) lStep);
			} else if (lStep instanceof Limit) {
				if (limitValue == null) limitValue = (Limit) lStep;
				else throw new ElasticsearchQueryException(" # Limit > 1");
			} else if (lStep instanceof Filter) {
				filterList.add((Filter) lStep);
			} else if (lStep instanceof Match) {
				matchList.add((Match) lStep);
			} else if (lStep instanceof GroupBy) {
				if (groupBy == null) groupBy = (GroupBy) lStep;
				else throw new ElasticsearchQueryException(" # GroupBy > 1");
			} else {
				throw new UnsupportedOperationException("operation unsupported");
			}
		}
		
		if (projection == null) throw new ElasticsearchQueryException("no projection founded",null);
		if (!sortList.isEmpty() && limitValue == null) throw new com.stratio.connector.meta.exception.UnsupportedOperationException("cannot sort: limit is required"); 

	}



	private void buildQuery(Client elasticClient) throws UnsupportedException, com.stratio.connector.meta.exception.UnsupportedOperationException {
		//TODO Multisearch? elasticClient.prepareMultiSearch().add(requestBuilder);
		//TODO if count or distinct => prepare Count?? or aggregation
		requestBuilder = elasticClient.prepareSearch();
		
		
		//SET QUERY
		QueryBuilder queryBuilder = MatchBuilderHelper.createMatchBuilder(matchList);
		FilterBuilder filterBuilder = FilterBuilderHelper.createFilterBuilder(filterList);			
		if(filterBuilder != null) {
			requestBuilder.setQuery(QueryBuilders.filteredQuery(queryBuilder,filterBuilder));
		}else requestBuilder.setQuery(queryBuilder);

		
		//MODIFY QUERY
		ProjectModifier.modify(requestBuilder, projection);
		LimitModifier.modify(requestBuilder, limitValue, searchType);
		if (!sortList.isEmpty()) SortModifier.modify(requestBuilder, sortList);
		
		if (groupBy != null) GroupByModifier.modify(requestBuilder, groupBy);

	}
	


	/**
	 * 
	 */
	private void setSearchType() {
		if(limitValue == null) searchType = SearchType.SCAN;
		else {
			//TODO IF SORT IS EMPTY=> SCAN o QUERY_THEN_FETCH o setSort(Empty)(default=>score?)
			searchType = (sortList.isEmpty()) ? SearchType.QUERY_THEN_FETCH : SearchType.QUERY_THEN_FETCH;
		}		
	}

	/**
	 * Queries for objects in a collection
     * @param elasticClient
     */

	public QueryResult executeQuery(Client elasticClient) {
		
		ElasticsearchResultSet resultSet = new ElasticsearchResultSet();		
		//resultSet.setColumnMetadata(projection.getColumnList());// needed?? //REVIEW comentado por que este metodo ya no existe.
		SearchResponse scrollResp;
		
		if(searchType == SearchType.QUERY_THEN_FETCH){ 
			scrollResp = requestBuilder.execute().actionGet();
			for (SearchHit hit : scrollResp.getHits()) {
				resultSet.add(createRow(hit));
		    }
		}else if ( searchType == SearchType.SCAN){
			//Prepare to SCAN
			scrollResp = requestBuilder.execute().actionGet();
			do{
                String scrollId = scrollResp.getScrollId();
                TimeValue keepAlive = new TimeValue(LimitModifier.SCAN_TIMEOUT_MILLIS);
                SearchScrollRequestBuilder searchScrollRequestBuilder = elasticClient.prepareSearchScroll(scrollId);
                SearchScrollRequestBuilder searchScrollRequestBuilder1 = searchScrollRequestBuilder.setScroll(keepAlive);
                scrollResp = searchScrollRequestBuilder1.execute().actionGet();
			    for (SearchHit hit : scrollResp.getHits()) {
			    	resultSet.add(createRow(hit));
			    }
			}while(scrollResp.getHits().getHits().length == 0);
			   
		}
		
		if(groupBy != null){
//			Aggregation agg = scrollResp.getAggregations().get("keys");
//			agg.
//			response.getAggregations().get("keys");
//			Collection<Terms.Bucket> buckets = terms.getBuckets();
		}
		
		
		return QueryResult.createQueryResult(resultSet);
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
		
		for (Map.Entry<String, SearchHitField> entry : hit.fields().entrySet())	{
			row.addCell(entry.getKey(), new Cell(entry.getValue().getValue()));
		}

		return row;
	}
	




}
