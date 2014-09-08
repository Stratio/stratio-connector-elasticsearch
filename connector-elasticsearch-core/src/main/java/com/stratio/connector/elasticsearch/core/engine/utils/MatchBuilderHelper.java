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

import java.util.ArrayList;

import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.stratio.connector.meta.Match;
import com.stratio.meta.common.exceptions.UnsupportedException;



public class MatchBuilderHelper{
	
	private MatchBuilderHelper(){
	}

	public static QueryBuilder createMatchBuilder(ArrayList<Match> matchList) throws UnsupportedException{
		
		QueryBuilder queryBuilder = null;
		BoolQueryBuilder boolQueryBuilder = null;
		
		if(matchList.isEmpty()){
			return QueryBuilders.matchAllQuery();
			//TODO wrapper with QueryBuilders.IdQuery?. Equivalent to requestQuery.setIndices?
		}else{
			
			
			boolQueryBuilder = (matchList.size() > 1) ? QueryBuilders.boolQuery() : null ;
			
			for(Match match: matchList){
				QueryBuilder localQueryBuilder;
				//TODO only exact terms query implemented
				
				localQueryBuilder = QueryBuilders.termsQuery(match.getField(),match.getTerms()).minimumMatch(match.getMinimumMatch());
				
				if (boolQueryBuilder == null) queryBuilder = localQueryBuilder;
				else {
					
					boolQueryBuilder.must(localQueryBuilder);
				}
				/*TODO if(match.computeScore())
				else*/
			}
			
			
		}
	
		if (boolQueryBuilder != null) return boolQueryBuilder;
		else return queryBuilder;

	}
}
