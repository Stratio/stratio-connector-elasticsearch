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

package com.stratio.connector.elasticsearch.core.engine.query;

import static com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier.SCAN_TIMEOUT_MILLIS;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.util.ColumnTypeHelper;
import com.stratio.connector.elasticsearch.core.engine.metadata.MetadataCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Select;

import com.stratio.crossdata.common.result.QueryResult;

/**
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryExecutor {

    /**
     * The log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * This method execute a query in elasticSearch.
     *
     * @param elasticClient  the elasticSearch Client.
     * @param requestBuilder the query to execute.
     * @param queryData      the queryData.
     * @return the query result.
     */

    public QueryResult executeQuery(Client elasticClient, SearchRequestBuilder requestBuilder,
            ConnectorQueryData queryData) throws ExecutionException {

        QueryResult queryResult = null;

        try {

            ResultSet resultSet = new ResultSet();
            SearchResponse scrollResp = requestBuilder.execute().actionGet();
            long countResult = 0;
            boolean isLimit = false;
            long limit = 0;
            boolean endQuery = false;
            if (queryData.getLimit() != null) {
                isLimit = true;
                limit = queryData.getLimit().getLimit();
            }

            MetadataCreator crossdatadataCreator = new MetadataCreator();
            resultSet.setColumnMetadata(crossdatadataCreator.createColumnMetadata(queryData));

            do {
                scrollResp = elasticClient.prepareSearchScroll(scrollResp.getScrollId())
                        .setScroll(new TimeValue(SCAN_TIMEOUT_MILLIS)).execute().actionGet();

                for (SearchHit hit : scrollResp.getHits()) {
                    if (isLimit && countResult == limit) {
                        endQuery = true;
                        break;
                    }
                    countResult++;
                    resultSet.add(createRow(hit, queryData));

                }

            } while (scrollResp.getHits().getHits().length != 0 && !endQuery);

            queryResult = QueryResult.createQueryResult(resultSet);
        } catch (IndexMissingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("The index not exists. The ES connector returns an empty QueryResult. " + e.getMessage());
            }
            queryResult = QueryResult.createQueryResult(new ResultSet());
        }
        return queryResult;
    }

    /**
     * This method creates a row.
     *
     * @param hit       the Elasticsearch SearchHit.
     * @param queryData
     * @return the row.
     */
    private Row createRow(SearchHit hit, ConnectorQueryData queryData) throws ExecutionException {

        Map<ColumnName, String> alias = returnAlias(queryData);
        Map<String, Object> fields = getFields(hit);
        Row row = setRowValues(queryData, alias, fields);

        return row;
    }

    /**
     * This method creates a row.
     *
     * @param queryData the query data.
     * @param alias     the alias.
     * @param fields    the fields.
     * @return a row.
     */
    private Row setRowValues(ConnectorQueryData queryData, Map<ColumnName, String> alias, Map<String, Object> fields)
            throws ExecutionException {
        Row row = new Row();
        Set<String> fieldNames;

        Select select = queryData.getSelect();
        if (select == null) {
            fieldNames = fields.keySet();
        } else {
            fieldNames = createFieldNames(select.getColumnMap().keySet());
        }
        for (String field : fieldNames) {
            Object value = fields.get(field);
            ColumnName columnName = new ColumnName(queryData.getProjection().getCatalogName(),
                    queryData.getProjection().getTableName().getName(), field);
            if (alias.containsKey(columnName)) {
                field = alias.get(columnName);
            }


            row.addCell(field, new Cell(
                    ColumnTypeHelper.getCastingValue(select.getTypeMapFromColumnName().get(columnName), value)));
        }
        return row;
    }

    /**
     * This method creates the fields names.
     *
     * @param columnNames the column names.
     * @return the field names.
     */
    private Set<String> createFieldNames(Set<ColumnName> columnNames) {
        Set<String> fieldNames = new LinkedHashSet<>();
        for (ColumnName columnName : columnNames) {
            fieldNames.add(columnName.getName());
        }
        return fieldNames;
    }

    /**
     * This method return the fields for a hit.
     *
     * @param hit the hit.
     * @return the fields.
     */
    private Map<String, Object> getFields(SearchHit hit) {
        Map<String, Object> fields = hit.getSource();

        if (fields == null) {
            fields = new HashMap<>();
            for (Map.Entry<String, SearchHitField> entry : hit.fields().entrySet()) {

                fields.put(entry.getKey(), entry.getValue().getValue());
            }
        }
        return fields;
    }

    /**
     * This method return the field alias.
     *
     * @param queryData the query data.
     * @return the alias.
     */
    private Map<ColumnName, String> returnAlias(ConnectorQueryData queryData) {
        Map<ColumnName, String> alias = Collections.EMPTY_MAP;
        if (queryData.getSelect() != null) {
            alias = queryData.getSelect().getColumnMap();
        }
        return alias;
    }

}
