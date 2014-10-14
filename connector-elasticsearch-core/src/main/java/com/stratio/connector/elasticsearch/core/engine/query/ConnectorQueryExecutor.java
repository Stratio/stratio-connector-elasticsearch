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

import static com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier.SCAN_TIMEOUT_MILLIS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ColumnName;

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
            ConnectorQueryData queryData) {

        SearchType searchType = queryData.getSearchType();

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
            resultSet.setColumnMetadata(createMetadata(queryData));
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

    private List<ColumnMetadata> createMetadata(ConnectorQueryData queryData) {

        List<ColumnMetadata> retunColumnMetadata = new ArrayList<>();

        for (ColumnName field : queryData.getSelect().getColumnMap().keySet()) {
            String columnName = field.getName();

            ColumnMetadata columnMetadata = new ColumnMetadata(queryData.getProjection().getTableName().getName(),
                    columnName, queryData.getSelect().getTypeMap().get(field.getQualifiedName()));
            columnMetadata.setColumnAlias(queryData.getSelect().getColumnMap().get(field));
            retunColumnMetadata.add(columnMetadata);
        }

        return retunColumnMetadata;
    }

    /**
     * This method creates a row.
     *
     * @param hit       the Elasticsearch SearchHit.
     * @param queryData
     * @return the row.
     */
    private Row createRow(SearchHit hit, ConnectorQueryData queryData) {

        Map<ColumnName, String> alias = returnAlias(queryData);
        Map<String, Object> fields = getFields(hit);
        Row row = setRowValues(queryData, alias, fields);

        return row;
    }

    private Row setRowValues(ConnectorQueryData queryData, Map<ColumnName, String> alias, Map<String, Object> fields) {
        Row row = new Row();
        Set<String> fieldNames;

        if (queryData.getSelect() == null) {
            fieldNames = fields.keySet();
        } else {
            fieldNames = createFieldNames(queryData.getSelect().getColumnMap().keySet());
        }
        for (String field : fieldNames) {
            Object value = fields.get(field);
            ColumnName columnName = new ColumnName(queryData.getProjection().getCatalogName(),
                    queryData.getProjection().getTableName().getName(), field);
            if (alias.containsKey(columnName)) {
                field = alias.get(columnName);
            }

            row.addCell(field, new Cell(value));
        }
        return row;
    }

    private Set<String> createFieldNames(Set<ColumnName> columnNames) {
        Set<String> fieldNames = new LinkedHashSet<>();
        for (ColumnName columnName : columnNames) {
            fieldNames.add(columnName.getName());
        }
        return fieldNames;
    }

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

    private Map<ColumnName, String> returnAlias(ConnectorQueryData queryData) {
        Map<ColumnName, String> alias = Collections.EMPTY_MAP;
        if (queryData.getSelect() != null) {
            alias = queryData.getSelect().getColumnMap();
        }
        return alias;
    }

}
