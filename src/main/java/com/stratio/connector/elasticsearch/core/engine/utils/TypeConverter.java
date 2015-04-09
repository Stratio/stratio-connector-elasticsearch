/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * Created by jmgomez on 24/11/14.
 */
public final class TypeConverter {

    public static final String ES_LONG = "long";
    /**
     * The elasticsearch boolean name.
     */
    public static final String ES_BOOLEAN = "boolean";
    /**
     * The elasticsearch double name.
     */
    public static final String ES_DOUBLE = "double";
    /**
     * The elasticsearch float name.
     */
    public static final String ES_FLOAT = "float";
    /**
     * The elasticsearch integer name.
     */
    public static final String ES_INTEGER = "integer";
    /**
     * The elasticsearch string name.
     */
    public static final String ES_STRING = "string";

    /**
     * Constructor.
     */
    private TypeConverter() {
    }

    /**
     * This method translates the crossdata columnType to ElasticSearch type.
     *
     * @param columnType
     *            the crossdata column type.
     * @return the ElasticSearch columnType.
     * @throws ExecutionException
     *             if the type is not supported.
     */
    public static String convert(ColumnType columnType) throws ExecutionException {

        String type = "";
        return type;
        /*switch (columnType) {
        case BIGINT:
            type = ES_LONG;
            break;
        case BOOLEAN:
            type = ES_BOOLEAN;
            break;
        case DOUBLE:
            type = ES_DOUBLE;
            break;
        case FLOAT:
            type = ES_FLOAT;
            break;
        case INT:
            type = ES_INTEGER;
            break;
        case TEXT:
        case VARCHAR:
            type = ES_STRING;
            break;
        default:
            throw new ExecutionException("The type [" + columnType + "] is not supported in ElasticSearch");
        }
        return type;*/
    }
}
