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

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.stratio.connector.meta.Match;
import com.stratio.meta.common.exceptions.UnsupportedException;



public class MatchBuilderHelper{
	
	private MatchBuilderHelper(){
	}

	public static QueryBuilder createMatchBuilder(ArrayList<Match> matchList) throws UnsupportedException{
		
		
		
		if(matchList.isEmpty()){
			return QueryBuilders.matchAllQuery();
		}else{
			throw new UnsupportedException("Not yet supported full text searchs");
			/*
			 * TODO
			 * QueryBuilder queryBuilder = null;
			BoolQueryBuilder boolQueryBuilder = (queries.length > 1) ? QueryBuilders.boolQuery() : null;		
			if(boolQueryBuilder != null) return boolQueryBuilder;
			else return queryBuilder;
			*/
		}
	

	}
}
