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

package com.stratio.connector.elasticsearch.core.engine.query.metadata;

import java.util.ArrayList;
import java.util.List;

import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.metadata.structures.ColumnMetadata;

/**
 * The responsibility of this class is crete the metadata.
 * Created by jmgomez on 15/10/14.
 */
public class MetadataCreator {

    /**
     * This method creates the column metadata.
     * @param queryData the queryData object.
     * @return the list with the column metadata.
     */
    public List<ColumnMetadata> createColumnMetadata(ConnectorQueryData queryData) {

        List<ColumnMetadata> retunColumnMetadata = new ArrayList<>();

        for (ColumnName field : queryData.getSelect().getColumnMap().keySet()) {
            String columnName = field.getQualifiedName();

            ColumnMetadata columnMetadata = new ColumnMetadata(
                    queryData.getProjection().getTableName().getQualifiedName(),
                    columnName, queryData.getSelect().getTypeMap().get(columnName));
            columnMetadata.setColumnAlias(queryData.getSelect().getColumnMap().get(field));
            retunColumnMetadata.add(columnMetadata);
        }

        return retunColumnMetadata;
    }
}
