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

import java.util.Map;
import java.util.Set;

import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import org.elasticsearch.action.search.SearchRequestBuilder;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * The responsibility of this class is to create a Select creator.
 *
 * @author darroyo
 */
public class SelectCreator {

    /**
     * This method modify the query to add a select.
     *
     * @param requestBuilder the request builder.
     * @param select         the select.
     */
    public void modify(SearchRequestBuilder requestBuilder, Select select) {

        Set<Selector> columnMetadataList = select.getColumnMap().keySet();

        if (columnMetadataList != null && !columnMetadataList.isEmpty()) {

            String[] fields = new String[columnMetadataList.size()];
            int i = 0;
            for (Selector selector : columnMetadataList) {
                if (SelectCreator.isFunction(selector, "count")) {
                    fields = new String[]{"_id"};
                    requestBuilder.setSize(0);
                    break;
                }else if (SelectCreator.isFunction(selector, "sub_field")){
                     fields[i] = calculateSubFieldName(selector);;
                }else{
                    fields[i] = selector.getColumnName().getName();
                }

                i++;

            }

            requestBuilder.addFields(fields);
        }
    }

    public static String calculateSubFieldName(Selector selector) {
        FunctionSelector functionSelector = (FunctionSelector) selector;
        String field = functionSelector.getFunctionColumns().get(0).getColumnName().getName();
        String subField = functionSelector.getFunctionColumns().get(1).getStringValue();
        return field +"."+subField;
    }

    /**
     * Return if the Selector is a function, and if the function name is the passed.
     *
     * @param selector
     * @param functionName
     * @return
     */
    public static boolean isFunction(Selector selector, String functionName) {

        if (!(selector instanceof FunctionSelector))
            return false;

        FunctionSelector functionSelector = (FunctionSelector) selector;
        return functionSelector.getFunctionName().equalsIgnoreCase(functionName);
    }


    public static boolean hasFunction(Map<Selector, String> columnMetadata, String functionName) {

        for (Selector selector : columnMetadata.keySet()) {
            if (isFunction(selector, functionName)) {
                return true;
            }
        }

        return false;
    }

    public static FunctionSelector getFunctionSelector(Map<Selector, String> columnMetadata, String functionName) {

        for (Selector selector : columnMetadata.keySet()) {
            if (isFunction(selector, functionName)) {
                return (FunctionSelector) selector;
            }
        }

        return null;
    }
}
