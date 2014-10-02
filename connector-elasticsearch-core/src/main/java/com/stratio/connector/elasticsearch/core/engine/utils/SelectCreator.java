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

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;

import com.stratio.meta.common.logicalplan.Select;
import com.stratio.meta2.common.data.ColumnName;

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
        for (ColumnName columnName :columnNames){
            fieldNames.add(columnName.getName());
        }
        return fieldNames;
    }
}
