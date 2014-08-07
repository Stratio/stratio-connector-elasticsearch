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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;

import com.stratio.connector.meta.Limit;
import com.stratio.meta.common.logicalplan.LogicalStep;

/**
 * @author darroyo
 *
 */
public class LimitModifier{

	//TODO move to configuration
	public static final int SCAN_TIMEOUT_MILLIS = 600000;
	//public static final int SIZE_QUERY_ANDTHEN_FETCH = 10;
	public static final int SIZE_SCAN = 10;
	
	
	private LimitModifier(){}
	public static void modify(SearchRequestBuilder requestBuilder, Limit limit) {
		if (limit != null) { requestBuilder.setSize(limit.getLimit()); 
		}else {
			requestBuilder.setScroll(new TimeValue(SCAN_TIMEOUT_MILLIS)).setSize(SIZE_SCAN).setSearchType(SearchType.SCAN);
		}
	}
	
	//TODO different requests?

}
