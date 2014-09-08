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
package com.stratio.connector.elasticsearch.core.engine.utils;

import java.util.ArrayList;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.stratio.connector.meta.Sort;
import com.stratio.meta.common.exceptions.ExecutionException;


/**
 * @author darroyo
 */
public class SortModifier {

    private SortModifier() {
    }

	public static void modify(SearchRequestBuilder requestBuilder, ArrayList<Sort> sortList) throws ExecutionException {
		boolean containsField= false;
		boolean containsScore = false;
		
		//TODO missings fields?
		for (Sort sortElem : sortList) { 
			//TODO scoreSort?
			if(sortElem.getType() == Sort.SCORE){
				requestBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
				containsScore = true;
			}
			else{
				SortOrder sOrder = ( sortElem.getType() == Sort.ASC ) ? SortOrder.ASC : SortOrder.DESC;
				requestBuilder.addSort(SortBuilders.fieldSort(sortElem.getField()).order( sOrder));
				containsField = true;
			}
		}	
		//TODO implement user-defined query?
		if(containsField && containsScore){
			requestBuilder.setTrackScores(true);
			// throw new ExecutionException("Sort by score and field unsupported");
		}
	}

}
