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

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.TableMetadata;

/**
 * The responsibility of this class is create a IndexRequestBuilder.
 * Created by jmgomez on 12/09/14.
 */
public class IndexRequestBuilderCreator {

    /**
     * Returns an IndexRequestBuilder.
     *
     * @param targetTable   the table where the row must be inserted.
     * @param elasticClient the connection to elastic search.
     * @param row           the row to insert.
     * @throws UnsupportedException if the opperation is not supported.
     */
    public IndexRequestBuilder createIndexRequestBuilder(TableMetadata targetTable, Client elasticClient, Row row)
            throws UnsupportedException {

        Map<String, Object> dataInsert = createInsertMap(row);
        String pk = createPrimaryKey(targetTable, dataInsert);
        IndexRequestBuilder indexRequestBuilder = createIndexRequestBuilder(elasticClient, targetTable, dataInsert, pk);

        return indexRequestBuilder;

    }

    /**
     * This method creates a indexrequesbuider with PK.
     *
     * @param elasticClient the connection to elastic search.
     * @param targetTable   the table where the row must be inserted.
     * @param dataInsert    the data to insert.
     * @param pk            the pk.
     * @return the index builder
     */
    private IndexRequestBuilder createIndexRequestBuilder(Client elasticClient, TableMetadata targetTable,
            Map<String, Object> dataInsert, String pk) {
        IndexRequestBuilder indexRequestBuilder;

        String index = targetTable.getName().getCatalogName().getName();
        String type = targetTable.getName().getName();

        if (pk != null) {
            indexRequestBuilder = elasticClient.prepareIndex(index, type, pk).setSource(dataInsert);
        } else {
            indexRequestBuilder = elasticClient.prepareIndex(index, type).setSource(dataInsert);
        }

        return indexRequestBuilder;
    }

    /**
     * Return the table PK.
     *
     * @param targetTable the table.
     * @param dataInsert  the data to insert.
     * @return the value of PK. Null if don't exist PK.
     * @throws UnsupportedException if the pk type is not supported.
     */
    private String createPrimaryKey(TableMetadata targetTable, Map<String, Object> dataInsert)
            throws UnsupportedException {
        String pk = null;

        for (Map.Entry<String, Object> rowName : dataInsert.entrySet()) {
            TableName tableName = targetTable.getName();

            if (targetTable.isPK(new ColumnName(tableName.getCatalogName().getName(), tableName.getName(),
                    rowName.getKey()))) {
                String tempPK = rowName.getValue().toString();
                checkPkTypeSupport(pk);
                pk = tempPK;
            }
        }

        return pk;
    }

    /**
     * Check if the PK type is support.
     *
     * @param pk the actual PK.
     * @throws UnsupportedException if the PK is not supported.
     */
    private void checkPkTypeSupport(String pk) throws UnsupportedException {
        if (pk != null) {
            throw new UnsupportedException("Only one PK is allowed");
        }

    }

    /**
     * this method converts a ROW in a MAP
     *
     * @param row the row.
     * @return the map from the row.
     */
    private Map<String, Object> createInsertMap(Row row) {
        Map<String, Object> dataInsert = new HashMap<String, Object>();
        for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
            Object cellValue = entry.getValue().getValue();
            dataInsert.put(entry.getKey(), cellValue);
        }

        return dataInsert;
    }
}
