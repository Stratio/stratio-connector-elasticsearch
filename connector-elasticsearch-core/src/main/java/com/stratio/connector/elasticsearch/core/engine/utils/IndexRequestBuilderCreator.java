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

import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jmgomez on 12/09/14.
 */
public class IndexRequestBuilderCreator {


    /**
     * Returns an IndexRequestBuilder. Adds the json created from the row
     *
     * @param targetTable   the table where the row must be inserted.
     * @param elasticClient the connection to elastic search.
     * @param row           the row to insert.
     * @throws UnsupportedException if the opperation is not supported.
     */
    public IndexRequestBuilder createIndexRequestBuilder(TableMetadata targetTable, Client elasticClient, Row row) throws UnsupportedException {


        Map<String, Object> dataInsert = createInsertMap(row);
        String pk = createPrimaryKey(targetTable, dataInsert);
        IndexRequestBuilder indexRequestBuilder = createIndexRequestBuilder(elasticClient, targetTable, dataInsert, pk);

        return indexRequestBuilder;


    }

    private IndexRequestBuilder createIndexRequestBuilder(Client elasticClient, TableMetadata targetTable, Map<String, Object> dataInsert, String pk) {
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
    private String createPrimaryKey(TableMetadata targetTable, Map<String, Object> dataInsert) throws UnsupportedException {
        String pk = null;

        for (String rowName : dataInsert.keySet()) {
            TableName tableName = targetTable.getName();

            if (targetTable.isPK(new ColumnName(tableName.getCatalogName().getName(), tableName.getName(), rowName))) {
                Object tempPK = dataInsert.get(rowName);
                checkPkTypeSupport(pk, tempPK);
                pk = (String) tempPK;
            }
        }

        return pk;
    }

    /**
     * Check if the PK type is support.
     *
     * @param pk     the actual PK.
     * @param tempPK the new PK.
     * @throws UnsupportedException if the PK is not supported.
     */
    private void checkPkTypeSupport(String pk, Object tempPK) throws UnsupportedException {
        if (pk != null) throw new UnsupportedException("Only one PK is allowed");
        if (!(tempPK instanceof String)) throw new UnsupportedException("The PK only can has String values");
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
