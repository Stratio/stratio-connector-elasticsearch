/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.elasticsearch.core.engine.utils;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.logicalplan.Select;

/**
 * @author darroyo
 */
public class SelectCreator {

    public void modify(SearchRequestBuilder requestBuilder, Select select) {

        Set<String> columnMetadataList = createFieldNames(select.getColumnMap().keySet());

        if (columnMetadataList != null && !columnMetadataList.isEmpty()) {

            String[] fields = new String[columnMetadataList.size()];
            int i = 0;
            for (String columnName : columnMetadataList) {
                String[] splitColumnName = columnName.split("\\.");
                fields[i] = splitColumnName[splitColumnName.length - 1];
                i++;
            }

            requestBuilder.addFields(fields);
        }
    }

    private Set<String> createFieldNames(Set<ColumnName> columnNames) {
        Set<String> fieldNames = new HashSet<>();
        for (ColumnName columnName : columnNames) {
            fieldNames.add(columnName.getName());
        }
        return fieldNames;
    }
}
