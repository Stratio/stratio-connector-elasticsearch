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
package com.stratio.connector.elasticsearch.core.engine.utils;

import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import com.stratio.connector.meta.ColumnGroupBy;
import com.stratio.connector.meta.GroupBy;
import com.stratio.meta.common.statements.structures.selectors.GroupByFunction;


/**
 * @author darroyo
 *
 */
public class GroupByModifier {
	
	private GroupByModifier(){}
	public static void modify(SearchRequestBuilder requestBuilder, GroupBy groupBy) {

		List<String> fieldsGroup = groupBy.getFieldsGroup();
		
		for(ColumnGroupBy columns : groupBy.getColumns()){
			String field = columns.getIdentifiers();
			GroupByFunction groupFunction = columns.getGroupByFunction();
			switch(groupFunction){
				case  SUM : 
					//AggregationBuilders.filter("").subAggregation(aggregation)
					//AggregationBuilders.global("").
					//AggregationBuilders.nested(name)
					//AggregationBuilders.terms(name)
					//SUBAGGREGATIONS=>http://elasticsearch-users.115913.n3.nabble.com/Java-API-for-multiple-sub-aggregations-td4055253.html
					//requestBuilder.addAggregation(AggregationBuilders.sum(columns.getAlias()).field(field).);
					//ValuesSourceAggregationBuilder<ValuesSourceAggregationBuilder<B>>
				break;
					
					
				case MIN : break;
				case MAX : break;
				case AVG : break;
				case COUNT : break;
				default : break;
			}
			
		}
		
		
	}
}
