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
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.commons.util.ColumnTypeHelper;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.elasticsearch.core.engine.metadata.MetadataCreator;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;

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
     * @param elasticClient
     *            the elasticSearch Client.
     * @param requestBuilder
     *            the query to execute.
     * @param queryData
     *            the queryData.
     * @return the query result.
     */

    public QueryResult executeQuery(Client elasticClient, SearchRequestBuilder requestBuilder, ProjectParsed queryData)
                    throws ExecutionException {

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

            queryResult = QueryResult.createQueryResult(resultSet, 0, true);
        } catch (IndexMissingException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("The index does not exist. The ES connector returns an empty QueryResult. "
                                + e.getMessage());
            }
            queryResult = QueryResult.createQueryResult(new ResultSet(), 0, true);
        }
        return queryResult;
    }

    /**
     * This method creates a row.
     *
     * @param hit
     *            the Elasticsearch SearchHit.
     * @param queryData
     * @return the row.
     */
    private Row createRow(SearchHit hit, ProjectParsed queryData) throws ExecutionException {

        Map<Selector, String> alias = returnAlias(queryData);
        Map<String, Object> fields = getFields(hit);
        Row row = setRowValues(queryData, alias, fields);

        return row;
    }

    /**
     * This method creates a row.
     *
     * @param queryData
     *            the query data.
     * @param alias
     *            the alias.
     * @param fields
     *            the fields.
     * @return a row.
     */
    private Row setRowValues(ProjectParsed queryData, Map<Selector, String> alias, Map<String, Object> fields)
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

            ColumnName columnName = new ColumnName(queryData.getProject().getCatalogName(), queryData.getProject()
                            .getTableName().getName(), field);

            ColumnSelector columnSelector = new ColumnSelector(columnName);
            for (Map.Entry<Selector, String> allAlias: alias.entrySet()){ //TODO improve this pice of code. The map
            // for column selector dont work fine.
                if (allAlias.getKey().getColumnName().getName().equals(columnName.getName())){
                    String aliasValue = allAlias.getValue();
                    if (aliasValue!=field){
                        columnSelector.setAlias(aliasValue);
                        field=aliasValue;
                    }
                    break;
                }
            }
            row.addCell(field,
                            new Cell(ColumnTypeHelper.getCastingValue(
                                    recoveredColumnType(select, columnSelector), value))); //TODO like before
        }

        logger.debug("Fields:" + row.getCells().toString());

        return row;
    }

    private ColumnType recoveredColumnType(Select select, ColumnSelector columnSelector) {
        ColumnType columntype = null;
        Map<Selector, ColumnType> typeMapFromColumnName = select.getTypeMapFromColumnName();
        for (Map.Entry<Selector, ColumnType> columnMap :typeMapFromColumnName.entrySet()){
            if (columnMap.getKey().getColumnName().getName().equals(columnSelector.getName().getName())){
                columntype = columnMap.getValue();
                break;
            }
        }
        return columntype;
    }

    /**
     * This method creates the field names.
     *
     * @param selectors
     *            the column names.
     * @return the field names.
     * @throws ExecutionException
     */
    private Set<String> createFieldNames(Set<Selector> selectors) throws ExecutionException {
        Set<String> fieldNames = new LinkedHashSet<>();
        for (Selector selector : selectors) {
            fieldNames.add((String) SelectorHelper.getRestrictedValue(selector, SelectorType.COLUMN));
        }
        return fieldNames;
    }

    /**
     * This method return the fields for a hit.
     *
     * @param hit
     *            the hit.
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
     * @param queryData
     *            the query data.
     * @return the alias.
     */
    private Map<Selector, String> returnAlias(ProjectParsed queryData) {
        Map<Selector, String> alias = Collections.emptyMap();
        if (queryData.getSelect() != null) {
            alias = queryData.getSelect().getColumnMap();
        }
        return alias;
    }

}
