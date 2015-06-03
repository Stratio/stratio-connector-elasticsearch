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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.elasticsearch.core.engine.metadata.ESIndexType;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * This class is responsible to create ContentBuilders. Created by jmgomez on 11/09/14.
 */
public class ContentBuilderCreator {

    /**
     * The properties identification.
     */
    private static final String PROPERTIES = "properties";
    /**
     * The type identification.
     */
    private static final String TYPE = "type";
    /**
     * The index identification.
     */
    private static final String INDEX = "index";


    private static final String FORMAT_FIELD = "format";

    private static final String ANALYZER_FIELDS = "analyzer";

    private static final String STORE = "store";

    /**
     * The id elasticsearch name.
     */
    private static final String ID = "_id";
    /**
     * The elasticsearch long name.
     */

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The XContentBuilder.
     */


    /**
     * This method creates the XContentBuilder for a type.
     *
     * @param typeMetadata the type crossdata.
     * @return the XContentBuilder that represent the type.
     * @throws ExecutionException if an error happen.
     */
    public XContentBuilder createTypeSource(TableMetadata typeMetadata) throws ExecutionException {

        XContentBuilder xContentBuilder = null;
        try {

            xContentBuilder = XContentFactory.jsonBuilder().startObject();
            createFieldOptions(typeMetadata, xContentBuilder);
            xContentBuilder.endObject();

            if (logger.isDebugEnabled()) {
                logger.debug("Crete type [" + typeMetadata.getName().getName() + "] in index ["
                        + typeMetadata.getName().getCatalogName() + "]");
                logger.debug("Mapping : " + xContentBuilder.string());
            }

        } catch (IOException e) {
            String msg = "Error create type crossdatadata. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        return xContentBuilder;
    }

    /**
     * This method create the xcontenBuilder for add a field in a mapping.
     *
     * @param columnMetadata the colimn meta data.
     * @return the XContentBuilder.
     * @throws IOException        if a IO excetion happens.
     * @throws ExecutionException if an error happen.
     */
    public XContentBuilder addColumn(ColumnMetadata columnMetadata) throws IOException, ExecutionException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject(columnMetadata.getName().getTableName().getName()).startObject(PROPERTIES)
                .startObject(columnMetadata.getName().getName())
                .field(TYPE, TypeConverter.convert(columnMetadata.getColumnType()))
                .field(INDEX, ESIndexType.getDefault().getCode()).endObject().endObject().endObject()
                .endObject();

        return mapping;
    }

    /**
     * This method creates the fields options.
     *
     * @param tableMetadata the table metadata.
     * @throws IOException        if an error happen creating the ContentBuilder.
     * @throws ExecutionException if an error happen
     */
    private void createFieldOptions(TableMetadata tableMetadata, XContentBuilder xContentBuilder) throws IOException, ExecutionException {

        configureIndex(ID, ESIndexType.NOT_ANALYZED, xContentBuilder);
        Map<ColumnName, ColumnMetadata> columns = tableMetadata.getColumns();
        if (columns != null && !columns.isEmpty()) {
            xContentBuilder.startObject(PROPERTIES);
            for (Map.Entry<ColumnName, ColumnMetadata> column : columns.entrySet()) {

                String name = column.getKey().getName();

                xContentBuilder = xContentBuilder.startObject(name);
                xContentBuilder = processColumnProperties(xContentBuilder, column.getValue());
                //xContentBuilder= xContentBuilder.endObject();
            }
            xContentBuilder.endObject();
        }

    }

    private XContentBuilder processColumnProperties(XContentBuilder xContentBuilder, ColumnMetadata columnMetadata) throws ExecutionException, IOException {
        String columnType = TypeConverter.convert(columnMetadata.getColumnType());
        xContentBuilder = xContentBuilder.field(TYPE, columnType);

        Map<String, List<String>> columProperties = columnMetadata.getColumnType().getColumnProperties();
        if (columProperties != null && !columProperties.containsKey(INDEX)) {
            xContentBuilder.field(INDEX, getTypeIndex(columnType));
        }

        if (columProperties != null){
            for ( String columProperty: columProperties.keySet()){
                xContentBuilder = xContentBuilder.field(columProperty, columProperties.get(columProperty));
            }
        }

        return xContentBuilder.endObject();
    }

    /**
     * Resolve the typeIndex.
     *
     * @param columnType the columnType.
     * @return the index type.
     */
    private String getTypeIndex(String columnType) {
        String stringType;
        if (TypeConverter.ES_BOOLEAN.equals(columnType)) {
            stringType = ESIndexType.NOT_ANALYZED.getCode();
        } else {
            stringType = ESIndexType.getDefault().getCode();

        }
        return stringType;
    }

    /**
     * This method create the index.
     *
     * @param field     the field to index.
     * @param indexType the index type.
     * @throws IOException if an error happen creating the ContentBuilder.
     */
    private void configureIndex(String field, ESIndexType indexType, XContentBuilder xContentBuilder) throws IOException {

        xContentBuilder.startObject(field).field(INDEX, indexType.getCode()).endObject();

    }

}
