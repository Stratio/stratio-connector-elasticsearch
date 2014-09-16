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
package com.stratio.connector.elasticsearch.core.configuration;


import com.stratio.meta.common.connector.IConfiguration;
import com.stratio.meta.common.connector.Operations;

import java.util.EnumMap;
import java.util.Map;

/**
 */
public class SupportedOperationsCreator implements IConfiguration {


    private static final Map<Operations, Boolean> support;


    //TODO
    static {

        support = new EnumMap<Operations, Boolean>(Operations.class);
        support.put(Operations.CREATE_CATALOG, Boolean.FALSE);
        support.put(Operations.DROP_CATALOG, Boolean.TRUE);
        support.put(Operations.CREATE_TABLE, Boolean.FALSE);
        support.put(Operations.DROP_TABLE, Boolean.TRUE);
        support.put(Operations.INSERT, Boolean.TRUE);
        support.put(Operations.DELETE, Boolean.FALSE);
        support.put(Operations.PROJECT, Boolean.TRUE);
        support.put(Operations.SELECT_WINDOW, Boolean.FALSE);
        support.put(Operations.SELECT_LIMIT, Boolean.FALSE);
        support.put(Operations.SELECT_INNER_JOIN, Boolean.FALSE);
        support.put(Operations.SELECT_ORDER_BY, Boolean.FALSE);
        support.put(Operations.SELECT_GROUP_BY, Boolean.FALSE);
        support.put(Operations.SELECT_FUNCTIONS, Boolean.FALSE);
        support.put(Operations.SELECT_WHERE_IN, Boolean.FALSE);
        support.put(Operations.SELECT_WHERE_BETWEEN, Boolean.FALSE);
        support.put(Operations.FILTER_PK_EQ, Boolean.FALSE);
        support.put(Operations.FILTER_PK_GT, Boolean.FALSE);
        support.put(Operations.FILTER_PK_LT, Boolean.FALSE);
        support.put(Operations.FILTER_PK_GET, Boolean.FALSE);
        support.put(Operations.FILTER_PK_LET, Boolean.FALSE);
        support.put(Operations.FILTER_PK_DISTINCT, Boolean.TRUE);
        support.put(Operations.FILTER_NON_INDEXED_EQ, Boolean.TRUE);
        support.put(Operations.FILTER_NON_INDEXED_GT, Boolean.TRUE);
        support.put(Operations.FILTER_NON_INDEXED_LT, Boolean.TRUE);
        support.put(Operations.FILTER_NON_INDEXED_GET, Boolean.TRUE);
        support.put(Operations.FILTER_NON_INDEXED_LET, Boolean.TRUE);
        support.put(Operations.FILTER_NON_INDEXED_DISTINCT, Boolean.TRUE);
        support.put(Operations.FILTER_INDEXED_EQ, Boolean.TRUE);
        support.put(Operations.FILTER_INDEXED_GT, Boolean.TRUE);
        support.put(Operations.FILTER_INDEXED_LT, Boolean.TRUE);
        support.put(Operations.FILTER_INDEXED_GET, Boolean.TRUE);
        support.put(Operations.FILTER_INDEXED_LET, Boolean.TRUE);
        support.put(Operations.FILTER_INDEXED_DISTINCT, Boolean.TRUE);
        support.put(Operations.FILTER_FULLTEXT, Boolean.FALSE);
        support.put(Operations.FILTER_FUNCTION_EQ, Boolean.FALSE);
        support.put(Operations.FILTER_FUNCTION_GT, Boolean.FALSE);
        support.put(Operations.FILTER_FUNCTION_LT, Boolean.FALSE);
        support.put(Operations.FILTER_FUNCTION_GET, Boolean.FALSE);
        support.put(Operations.FILTER_FUNCTION_LET, Boolean.FALSE);
        support.put(Operations.FILTER_FUNCTION_DISTINCT, Boolean.FALSE);
    }


    private SupportedOperationsCreator() {
    }

    /**
     * Return the supported operations.
     *
     * @return the supported operations
     */
    public static Map<Operations, Boolean> getSupportedOperations() {
        return support;
    }


}
