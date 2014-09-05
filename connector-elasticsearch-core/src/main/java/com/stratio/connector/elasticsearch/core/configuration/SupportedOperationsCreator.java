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
        support.put(Operations.CREATE_CATALOG, Boolean.FALSE);//FALSE?
        support.put(Operations.CREATE_TABLE, Boolean.FALSE);
        support.put(Operations.DELETE, Boolean.TRUE);
        support.put(Operations.DROP_CATALOG, Boolean.TRUE);
        support.put(Operations.DROP_TABLE, Boolean.TRUE);
        support.put(Operations.INSERT, Boolean.TRUE);
        //support.put(Operations.INSERT_BULK, Boolean.TRUE);
//        support.put(Operations.SELECT_AGGREGATION_SELECTORS, Boolean.FALSE);
        support.put(Operations.SELECT_GROUP_BY, Boolean.FALSE);
        support.put(Operations.SELECT_INNER_JOIN, Boolean.FALSE);
        support.put(Operations.SELECT_LIMIT, Boolean.TRUE);
        support.put(Operations.SELECT_ORDER_BY, Boolean.TRUE);
        support.put(Operations.SELECT_WHERE_BETWEEN, Boolean.TRUE);
        //      support.put(Operations.SELECT_WHERE_MATCH, Boolean.TRUE);
        support.put(Operations.SELECT_WINDOW, Boolean.FALSE);
        support.put(Operations.SELECT_WHERE_IN, Boolean.TRUE);
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
