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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
/**
 * This class is responsible to create ContentBuilders'
 * Created by jmgomez on 11/09/14.
 */
public class ContentBuilderCreator {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The XContentBuilder.
     */

    private  XContentBuilder xContentBuilder;
    /**
     * This method creates the XContentBuilder for a type.
     *
     * @param typeMetadata the type crossdatadata.
     * @return the XContentBuilder that represent the type.
     * @throws UnsupportedException if the type crossdatadata is not supported.
     * @thros ExecutionException if a error occurs.
     */
    public XContentBuilder createTypeSource(TableMetadata typeMetadata)
            throws UnsupportedException, ExecutionException {


        try {

            xContentBuilder = XContentFactory.jsonBuilder().startObject();


            createFieldOptions(typeMetadata);

            xContentBuilder.endObject();

            if (logger.isDebugEnabled()) {
                logger.debug("Crete type [" + typeMetadata.getName().getName() + "] in index [" + typeMetadata.getName()
                        .getCatalogName() + "]");
                logger.debug("Mapping : " + xContentBuilder.string());
            }

        } catch (IOException e) {
            String msg = "Error create type crossdatadata. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        return xContentBuilder;
    }



    private void createFieldOptions(TableMetadata typeMetadata)
            throws IOException, UnsupportedException {

        createId(xContentBuilder);
        Map<ColumnName, ColumnMetadata> columns = typeMetadata.getColumns();
        if (columns != null && !columns.isEmpty()) {
            xContentBuilder.startObject("properties");
            for (Map.Entry<ColumnName, ColumnMetadata> column : columns.entrySet()) {
                String columnType = convertType(column.getValue().getColumnType());
                String name = column.getKey().getName();
                xContentBuilder = xContentBuilder.startObject(name).field("type", columnType).endObject();
            }
            xContentBuilder.endObject();
        }

    }

    private void createId(XContentBuilder xContentBuilder) throws IOException {

        xContentBuilder.startObject("_id").field("index", "not_analyzed").endObject();

    }

    /**
     * This method translates the crossdata columnType to ElasticSearch type.
     *
     * @param columnType the crossdata column type.
     * @return the ElasticSearch columnType.
     * @throws UnsupportedException if the type is not supported.
     */
    private String convertType(ColumnType columnType) throws UnsupportedException {

        String type = "";
        switch (columnType) {
        case BIGINT:
            type = "long";
            break;
        case BOOLEAN:
            type = "boolean";
            break;
        case DOUBLE:
            type = "double";
            break;
        case FLOAT:
            type = "float";
            break;
        case INT:
            type = "integer";
            break;
        case TEXT:
        case VARCHAR:
            type = "string";
            break;
        default:
            throw new UnsupportedException("The type [" + columnType + "] is not supported in ElasticSearch");
        }
        return type;
    }
}
