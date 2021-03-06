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

import com.stratio.connector.elasticsearch.core.engine.metadata.ESIndexType;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.metadata.TableMetadata;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible to create ContentBuilders. Created by jmgomez on 11/09/14.
 */
public class ContentBuilderCreator {

    // Logger
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    // Elasticsearch mapping properties
    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String FIELDS = "fields";
    private static final String INDEX = "index";
    private static final String DYNAMIC = "dynamic";
    private static final String ANALYZER = "analyzer";

    // Elasticsearch specific fields
    //private static final String ID = "_id";

    // Crossdata parsing properties
    private static final String ANALYZER_FIELDS = "analyzer";

    // Other
    /*
       private static final String FORMAT_FIELD = "format";
       private static final String STORE = "store";
       */


   /**
     * This method creates the XContentBuilder defining the mapping associated to an elasticsearch type.
     *
     * @param typeMetadata object that defines the different fields and properties that define the elasticsearch type.
     * @return the XContentBuilder that represent the type.
     * @throws ExecutionException if an error happen.
     */
    public XContentBuilder createTypeSource(TableMetadata typeMetadata) throws ExecutionException {

        XContentBuilder xContentBuilder;
        try {
            // Creates the XContentBuilder that defines the type mapping
            xContentBuilder = XContentFactory.jsonBuilder().startObject();
            createFieldOptions(typeMetadata, xContentBuilder);
            xContentBuilder.endObject();

            if (logger.isDebugEnabled()) {
                logger.debug("Created type [" + typeMetadata.getName().getName() + "] in index ["
                        + typeMetadata.getName().getCatalogName() + "]");
                logger.debug("Mapping : " + xContentBuilder.string());
            }

        } catch (IOException e) {
            String msg = "Error creating type crossdata. " + e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg, e);
        }

        return xContentBuilder;
    }

   /**
     * This calls the methods necessary to add both generic and field specific properties to the mapping.
     *
     * @param tableMetadata the table metadata.
     * @throws IOException        if an error happen creating the ContentBuilder.
     * @throws ExecutionException if an error happen
     */
    private void createFieldOptions(TableMetadata tableMetadata, XContentBuilder xContentBuilder) throws IOException, ExecutionException {

        // Adds generic properties to the mapping
        configureIndex(xContentBuilder);

        // For every defined field the specific properties are added to the mapping by calling the appropriate method
        Map<ColumnName, ColumnMetadata> columns = tableMetadata.getColumns();
        if (columns != null && !columns.isEmpty()) {
            xContentBuilder.startObject(PROPERTIES);
            for (Map.Entry<ColumnName, ColumnMetadata> column : columns.entrySet()) {
                String name = column.getKey().getName();

                xContentBuilder = xContentBuilder.startObject(name);
                xContentBuilder = processColumnProperties(xContentBuilder, column.getValue());
            }
            xContentBuilder.endObject();
        }
    }


   /**
     * This method adds generic properties to the mapping.
     *
     * @param xContentBuilder   object defining the mapping
     * @throws IOException if an error happen creating the ContentBuilder.
     */
    private void configureIndex(XContentBuilder xContentBuilder) throws IOException {
        // Sets "dynamic" property to "false" in order to avoid fields not defined through the mappings being indexed
        xContentBuilder.field(DYNAMIC, "strict");
    }


   /**
     * Method that creates the mapping configurations for an specific field. Sets the type, any optional properties and, if the field has analyzers,
     * creates the appropriate sub-fields in order to support multiple field analysis
     *
     * @param xContentBuilder   object defining the mapping
     * @param columnMetadata    object defining the properties for each field
     * @return  modified XContentBuilder object
     * @throws ExecutionException
     * @throws IOException
     */
    private XContentBuilder processColumnProperties(XContentBuilder xContentBuilder, ColumnMetadata columnMetadata)
            throws ExecutionException, IOException {
        ColumnType columnType =  columnMetadata.getColumnType();
        Map<String, List<String>> columnTypeProperties = columnType.getColumnProperties();
        String columnTypeName = TypeConverter.convert(columnType);

        // Creates base field type
        xContentBuilder = xContentBuilder.field(TYPE, columnTypeName);

        // Apply specific properties for the given field
        if (columnTypeProperties != null) {
            // Retrieves any possible analyzers in order to be processed apart
            List<String> analyzers = columnTypeProperties.remove(ANALYZER_FIELDS);

            // Any property not being an analyzer is applied to the main field
            for ( Map.Entry<String, List<String>> entry: columnTypeProperties.entrySet()){
                if (entry.getValue().size() > 1){
                    xContentBuilder = xContentBuilder.field( entry.getKey(), entry.getValue());
                }
                else if (columnTypeProperties.get(entry.getKey()).size() == 1){
                    xContentBuilder = xContentBuilder.field(entry.getKey(), entry.getValue().get(0));
                }
            }

            // Once every property for the main field has been processed analyzers are checked in order to create specific sub-fields
            if (null != analyzers && !analyzers.isEmpty()){
                xContentBuilder.startObject(FIELDS);
                // For each defined analyzer a sub-field is created named the same as the analyzer and having the same type as the parent field.
                for (String analyzer : analyzers) {
                    xContentBuilder.startObject(analyzer);
                    xContentBuilder = xContentBuilder.field(TYPE, columnTypeName);
                    xContentBuilder = xContentBuilder.field(ANALYZER, analyzer);
                    xContentBuilder.endObject();
                }
                xContentBuilder.endObject();
            }
        }

        return xContentBuilder.endObject();
    }


    /**
     * This method creates the XContenBuilder that defines a mapping for a specific index and type.
     *
     * @param columnMetadata the column meta data.
     * @return the XContentBuilder.
     * @throws IOException        if a IO excetion happens.
     * @throws ExecutionException if an error happen.
     */
    public XContentBuilder addColumn(ColumnMetadata columnMetadata) throws IOException, ExecutionException {
        XContentBuilder mapping;

        // Adds the information about the index and type
        mapping = XContentFactory.jsonBuilder().startObject()
                .startObject(columnMetadata.getName().getTableName().getName()).startObject(PROPERTIES)
                .startObject(columnMetadata.getName().getName())
                .field(TYPE, TypeConverter.convert(columnMetadata.getColumnType()))
                .field(INDEX, ESIndexType.getDefault().getCode()).endObject().endObject().endObject()
                .endObject();

        return mapping;
    }
}
