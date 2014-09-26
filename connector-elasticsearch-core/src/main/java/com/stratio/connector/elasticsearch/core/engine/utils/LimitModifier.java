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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;

/**
 * @author darroyo
 */
public class LimitModifier {

    public static final int SCAN_TIMEOUT_MILLIS = 600000;
    public static final int SIZE_SCAN = 10;

    public void modify(SearchRequestBuilder requestBuilder, ConnectorQueryData type) {
            requestBuilder.setScroll(new TimeValue(SCAN_TIMEOUT_MILLIS)).setSize(SIZE_SCAN)
                    .setSearchType(SearchType.SCAN);
       


    }

}
