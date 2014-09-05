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

import com.stratio.meta.common.exceptions.UnsupportedException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;


public class MatchBuilderHelper {

    private MatchBuilderHelper() {
    }

    public static QueryBuilder createMatchBuilder(ArrayList/*<Match>*/ matchList) throws UnsupportedException {


        if (matchList.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        } else {
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
