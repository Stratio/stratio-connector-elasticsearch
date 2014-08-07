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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.Sort;

/**
 * @author darroyo
 *
 */
public class SortModifier{

	private SortModifier(){}
	public static void modify(SearchRequestBuilder requestBuilder, ArrayList<Sort> sortList) {
		
		//TODO missings fields?
		for (Sort sortElem : sortList) { 
			SortOrder sOrder = ( sortElem.getType() == Sort.ASC ) ? SortOrder.ASC : SortOrder.DESC;
			requestBuilder.addSort(SortBuilders.fieldSort(sortElem.getField()).order( sOrder));
		}	
	}

}
