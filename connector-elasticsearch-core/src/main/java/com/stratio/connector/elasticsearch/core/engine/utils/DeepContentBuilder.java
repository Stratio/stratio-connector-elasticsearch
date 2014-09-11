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

import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.ColumnType;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


/**
 * This class is responsible to create ContentBuilders'
 * Created by jmgomez on 11/09/14.
 */
public class DeepContentBuilder {

    /**
     * The Log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * This method creates the XContentBuilder for a type.
     * @param typeMetadata the type metadata.
     * @return the XContentBuilder that represent the type.
     * @throws UnsupportedException if the type metadata is not supported.
     * @thros ExecutionException if a error occurs.
     */
    public XContentBuilder createTypeSource(TableMetadata typeMetadata) throws UnsupportedException, ExecutionException {

        XContentBuilder xContentBuilder = null;
        try {

            xContentBuilder =  XContentFactory.jsonBuilder().startObject().startObject("properties");

            Map<ColumnName, ColumnMetadata> columns = typeMetadata.getColumns();
            for (ColumnName column: columns.keySet()){
               String columnType = convertType(columns.get(column).getColumnType());
                String name = column.getName();
                xContentBuilder =  xContentBuilder.startObject(name).field("type",columnType).endObject();
            }
            xContentBuilder = xContentBuilder.endObject().endObject();

            if (logger.isDebugEnabled()) {
                logger.debug("Crete type ["+typeMetadata.getName().getName()+"] in index ["+typeMetadata.getName().getCatalogName()+"]");
                logger.debug("Mapping : "+xContentBuilder.string());
            }

        } catch (IOException e) {
            String msg ="Error create type metadata. "+e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg,e);
        }



        return xContentBuilder;
    }

    /**
     * This method translates the meta columnType to ElasticSearch type.
     * @param columnType the meta column type.
     * @return the ElasticSearch columnType.
     * @throws UnsupportedException if the type is not supported.
     */
    private String convertType(ColumnType columnType) throws UnsupportedException {

        String type = "";
        switch (columnType){
            case BIGINT:  type ="long";break;
            case BOOLEAN:type ="boolean";break;
            case DOUBLE:type ="double";break;
            case FLOAT:type ="float";break;
            case INT: type="integer";break;
            case TEXT:
            case VARCHAR: type="string";break;
            default: throw new UnsupportedException("The typo ["+columnType+"] is not supported in ElasticSearch");
        }
        return type;
    }
}
