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

package com.stratio.connector.elasticsearch.core.engine.metadata;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.stratio.connector.elasticsearch.core.engine.utils.ContentBuilderCreator;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 24/11/14.
 */
public class AddColumnHandler implements AlterTableHandler {

    private final AlterOptions alterOptions;

    public AddColumnHandler(AlterOptions alterOptions) {
        this.alterOptions = alterOptions;
    }

    @Override public void execute(TableName tableName, Client connection) throws UnsupportedException {

        try {
            ContentBuilderCreator contentBuilderCreator = new ContentBuilderCreator();
            XContentBuilder source = null;
            source = contentBuilderCreator.addColumn(alterOptions.getColumnMetadata());
            connection.admin().indices().preparePutMapping(tableName.getCatalogName().getName()).setType(tableName
                    .getName()).setSource(source).execute().actionGet();

        } catch (IOException e) {
            //TODO
        }

    }
}
