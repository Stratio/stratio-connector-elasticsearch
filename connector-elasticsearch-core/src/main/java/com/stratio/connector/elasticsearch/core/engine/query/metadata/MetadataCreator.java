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

package com.stratio.connector.elasticsearch.core.engine.query.metadata;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta2.common.data.ColumnName;

/**
 * Created by jmgomez on 15/10/14.
 */
public class MetadataCreator {


    public List<ColumnMetadata> createMetadata(ConnectorQueryData queryData) {

        List<ColumnMetadata> retunColumnMetadata = new ArrayList<>();

        for (ColumnName field : queryData.getSelect().getColumnMap().keySet()) {
            String columnName = field.getQualifiedName();

            ColumnMetadata columnMetadata = new ColumnMetadata(queryData.getProjection().getTableName().getQualifiedName(),
                    columnName, queryData.getSelect().getTypeMap().get(columnName));
            columnMetadata.setColumnAlias(queryData.getSelect().getColumnMap().get(field));
            retunColumnMetadata.add(columnMetadata);
        }

        return retunColumnMetadata;
    }
}