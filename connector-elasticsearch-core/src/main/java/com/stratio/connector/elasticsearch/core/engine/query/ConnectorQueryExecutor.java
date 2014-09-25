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

package com.stratio.connector.elasticsearch.core.engine.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;

import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.QualifiedNames;
import static com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier.SCAN_TIMEOUT_MILLIS;
/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryExecutor {



    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());
    private boolean isMetadataCreate = false;

    /**
     * This method execute a query in elasticSearch.
     *
     * @param elasticClient  the elasticSearch Client.
     * @param requestBuilder the query to execute.
     * @param queryData      the queryData.
     * @return the query result.
     */

    public QueryResult executeQuery(Client elasticClient, SearchRequestBuilder requestBuilder,
            ConnectorQueryData queryData) {

        SearchType searchType = queryData.getSearchType();

        QueryResult queryResult = null;
        try {

            ResultSet resultSet = new ResultSet();
            SearchResponse scrollResp = requestBuilder.execute().actionGet();

            do {
                   scrollResp = elasticClient.prepareSearchScroll(scrollResp.getScrollId())
                            .setScroll(new TimeValue(SCAN_TIMEOUT_MILLIS)).execute().actionGet();




                for (SearchHit hit : scrollResp.getHits()) {
                    resultSet.add(createRow(hit, queryData));
                    resultSet.setColumnMetadata(creteMetadata(queryData));
                }

            } while (scrollResp.getHits().getHits().length != 0);

            queryResult = QueryResult.createQueryResult(resultSet);
        } catch (IndexMissingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("The index not exists. The ES connector returns an empty QueryResult. " + e.getMessage());
            }
            queryResult = QueryResult.createQueryResult(new ResultSet());
        }
        return queryResult;
    }


    private List<ColumnMetadata> creteMetadata(ConnectorQueryData queryData) {


        List<ColumnMetadata> retunColumnMetadata = new ArrayList<>();
        if (!isMetadataCreate){


            for (String field: queryData.getSelect().getColumnMap().keySet()){
                if (field.contains(".")){
                    String[] aField = field.split("\\.");
                    field = aField[aField.length-1];
                }

                ColumnMetadata columnMetadata = new ColumnMetadata(queryData.getProjection().getTableName().getName(),
                        field,queryData.getSelect().getTypeMap().get(field));
                columnMetadata.setColumnAlias(queryData.getSelect().getColumnMap().get(field));
                retunColumnMetadata.add(columnMetadata);
            }

            isMetadataCreate = true;
        }

        return retunColumnMetadata;
    }



    /**
     * This method creates a row from a mongoResult
     *
     * @param hit       the Elasticsearch SearchHit.
     * @param queryData
     * @return the row.
     */
    private Row createRow(SearchHit hit, ConnectorQueryData queryData) {


        Map<String, String> alias = returnAlias(queryData);
        Map<String, Object> fields = getFields(hit);
        Row row = setRowValues(queryData, alias, fields);

        return row;
    }

    private Row setRowValues(ConnectorQueryData queryData, Map<String, String> alias, Map<String, Object> fields) {
        Row row = new Row();
        Set<String> fieldNames;

        if(queryData.getSelect()==null) {
            fieldNames = fields.keySet();
        }else{
            fieldNames = queryData.getSelect().getColumnMap().keySet();
        }
        for (String field : fieldNames) {
            if (field.contains(".")){
                String[] aField = field.split("\\.");
                field = aField[aField.length-1];
            }
            Object value = fields.get(field);
            Project projection = queryData.getProjection();
            String qualifiedFieldName = QualifiedNames
                    .getColumnQualifiedName(projection.getCatalogName(), projection.getTableName().getName(), field);

            if (alias.containsKey(qualifiedFieldName)) {
                field = alias.get(qualifiedFieldName);
            }

            row.addCell(field, new Cell(value));
        }
        return row;
    }

    private Map<String, Object> getFields(SearchHit hit) {
        Map<String, Object> fields = hit.getSource();

        if (fields == null) {
            fields = new HashMap<>();
            for (Map.Entry<String, SearchHitField> entry : hit.fields().entrySet()) {

                fields.put(entry.getKey(), entry.getValue().getValue());
            }
        } return fields;
    }

    private Map<String, String> returnAlias(ConnectorQueryData queryData) {
        Map<String, String> alias = Collections.EMPTY_MAP;
        if (queryData.getSelect() != null) {
            alias = queryData.getSelect().getColumnMap();
        }
        return alias;
    }

}
