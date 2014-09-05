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

import com.stratio.connector.meta.Limit;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;

/**
 * @author darroyo
 */
public class LimitModifier {

    //TODO move to configuration
    public static final int SCAN_TIMEOUT_MILLIS = 600000;
    //public static final int SIZE_QUERY_ANDTHEN_FETCH = 10;
    public static final int SIZE_SCAN = 10;


    private LimitModifier() {
    }

    public static void modify(SearchRequestBuilder requestBuilder, Limit limit, SearchType type) {
        if (limit != null) {
            if (type == SearchType.SCAN) {
                requestBuilder.setScroll(new TimeValue(SCAN_TIMEOUT_MILLIS)).setSize(SIZE_SCAN).setSearchType(SearchType.SCAN);
            } else if (type == SearchType.QUERY_THEN_FETCH) {
                requestBuilder.setSize(limit.getLimit()).setSearchType(SearchType.QUERY_THEN_FETCH);
            }
            //TODO else throw new ExecutionException("SearchType unexpected: "+ type);
        } else {

            requestBuilder.setScroll(new TimeValue(SCAN_TIMEOUT_MILLIS)).setSize(SIZE_SCAN).setSearchType(SearchType.SCAN);
        }

    }

    //TODO different requests?

}
