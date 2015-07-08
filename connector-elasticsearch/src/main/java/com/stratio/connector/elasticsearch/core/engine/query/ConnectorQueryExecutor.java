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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.stratio.connector.elasticsearch.core.engine.utils.SelectCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.SelectorUtils;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
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

public class ConnectorQueryExecutor {

    /**
     * The log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private MetadataCreator crossdatadataCreator = new MetadataCreator();

    /**
     * This method execute a query in elasticSearch.
     *
     * @param elasticClient        the elasticSearch Client.
     * @param actionRequestBuilder the query to execute.
     * @param queryData            the queryData.
     * @return the query result.
     */

    public QueryResult executeQuery(Client elasticClient, ActionRequestBuilder actionRequestBuilder, ProjectParsed queryData)
            throws ExecutionException {

        QueryResult queryResult = null;
        SearchRequestBuilder requestBuilder = (SearchRequestBuilder) actionRequestBuilder;

        try {

            ResultSet resultSet = new ResultSet();
            SearchResponse response = ((SearchRequestBuilder) requestBuilder).execute().actionGet();
            resultSet.setColumnMetadata(crossdatadataCreator.createColumnMetadata(queryData));

            if (queryData.getGroupBy() != null && !queryData.getGroupBy().getIds().isEmpty()) {
                processAggregation(queryData, resultSet, response);
            } else if (SelectorUtils.hasFunction(queryData.getSelect().getColumnMap(), "count")) {
                processCount(queryData, resultSet, response);
            } else {
                processResults(queryData, resultSet, response);
            }
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

    private void processAggregation(ProjectParsed queryData, ResultSet resultSet, SearchResponse response) throws ExecutionException {

        for (Aggregation aggregation : response.getAggregations()) { //TODO support for multiple aggregations
            StringTerms stringTerms = (StringTerms) aggregation; //TODO support for different types
            Map<Selector, String> alias = returnAlias(queryData);

            for (Terms.Bucket bucket : stringTerms.getBuckets()) {
                Map<String, Object> fields = new HashMap();

                fields.put(stringTerms.getName(), bucket.getKey());
                if (bucket.getAggregations().iterator().hasNext()) {
                    processSubAggregation(queryData, resultSet, bucket, alias, fields);
                } else {
                    fields.put("count", bucket.getDocCount());
                    Row row = buildRow(queryData, alias, fields);
                    resultSet.add(row);
                }
            }
        }
    }

    private void processSubAggregation(ProjectParsed queryData, ResultSet resultSet, Terms.Bucket bucket, Map<Selector, String> alias, Map<String, Object> fields) throws ExecutionException {
        for (Aggregation subAgg : bucket.getAggregations().asList()) {
            StringTerms stringTermsSubAgg = (StringTerms) subAgg;
            for (Terms.Bucket subBucker : stringTermsSubAgg.getBuckets()) {
                fields.put("count", subBucker.getDocCount());
                fields.put(stringTermsSubAgg.getName(), subBucker.getKey());
                Row row = buildRow(queryData, alias, fields);
                resultSet.add(row);
            }
        }
    }

    private void processResults(ProjectParsed queryData, ResultSet resultSet, SearchResponse response) throws ExecutionException {
        for (SearchHit hit : response.getHits().getHits()) {
            resultSet.add(createRow(hit, queryData));
        }
    }

    private void processCount(ProjectParsed queryData, ResultSet resultSet, SearchResponse response) throws ExecutionException {
        FunctionSelector functionSelector = SelectorUtils.getFunctionSelector(queryData.getSelect().getColumnMap(), "count");
        Map<Selector, String> alias = returnAlias(queryData);
        Map<String, Object> fields = new HashMap();

        fields.put(functionSelector.getAlias(), response.getHits().totalHits());
        Row row = buildRow(queryData, alias, fields);
        resultSet.add(row);
    }

    /**
     * This method creates a row.
     *
     * @param hit       the Elasticsearch SearchHit.
     * @param queryData
     * @return the row.
     */
    private Row createRow(SearchHit hit, ProjectParsed queryData) throws ExecutionException {

        Map<Selector, String> alias = returnAlias(queryData);
        Map<String, Object> fields = getFields(hit);
        Row row = buildRow(queryData, alias, fields);

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
    private Row buildRow(ProjectParsed queryData, Map<Selector, String> alias, Map<String, Object> fields)
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
            field = pickAliasOrFieldName(alias, field, columnName, columnSelector);
            row.addCell(field, buildCell(select, value, columnSelector));
        }

        logger.debug("Fields:" + row.getCells().toString());

        return row;
    }

    private Cell buildCell(Select select, Object value, ColumnSelector columnSelector) throws ExecutionException {
        ColumnType columnType = recoveredColumnType(select, columnSelector);
        Object cellValue = ColumnTypeHelper.getCastingValue(columnType, value);
        return new Cell(cellValue);
    }

    private String pickAliasOrFieldName(Map<Selector, String> alias, String field, ColumnName columnName, ColumnSelector columnSelector) {
        for (Map.Entry<Selector, String> allAlias : alias.entrySet()) {
            // for column selector dont work fine.

            if (SelectorUtils.isFunction(allAlias.getKey(), "sub_field")
                    && SelectorUtils.calculateSubFieldName(allAlias.getKey()).equals(field)) {
                field = allAlias.getKey().getAlias();
            }else if (allAlias.getKey().getColumnName().getName().equals(columnName.getName())) {
                String
                        aliasValue = allAlias.getValue();
                if (!aliasValue.equals(field)) {
                    columnSelector.setAlias(aliasValue);
                    field = aliasValue;
                }
                break;
            }
        }
        return field;
    }

    private ColumnType recoveredColumnType(Select select, ColumnSelector columnSelector) {
        ColumnType columntype = null;
        Map<Selector, ColumnType> typeMapFromColumnName = select.getTypeMapFromColumnName();
        for (Map.Entry<Selector, ColumnType> columnMap : typeMapFromColumnName.entrySet()) {
            if (columnMap.getKey().getColumnName().getName().equals(columnSelector.getName().getName())) {
                columntype = columnMap.getValue();
                break;
            }else if(SelectorUtils.isFunction(columnMap.getKey(), "sub_field") &&
                    columnSelector.getColumnName().getName().contains(SelectorUtils.calculateSubFieldName(columnMap.getKey()))){
                columntype = columnMap.getValue();
            }
        }
        return columntype;
    }

    /**
     * This method creates the field names.
     *
     * @param selectors the column names.
     * @return the field names.
     * @throws ExecutionException
     */
    private Set<String> createFieldNames(Set<Selector> selectors) throws ExecutionException {
        Set<String> fieldNames = new LinkedHashSet<>();
        for (Selector selector : selectors) {
            if (SelectorUtils.isFunction(selector, "count")) {
                fieldNames.add((String) selector.getAlias());
            } else if (SelectorUtils.isFunction(selector, "sub_field")){
                fieldNames.add(SelectorUtils.calculateSubFieldName(selector));
            }else {
                fieldNames.add((String) SelectorHelper.getRestrictedValue(selector, SelectorType.COLUMN));
            }
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

                if (entry.getValue().getValues().size()>1){
                    fields.put(entry.getKey(), entry.getValue().getValues());
                }else{
                    fields.put(entry.getKey(), entry.getValue().getValue());
                }

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
    private Map<Selector, String> returnAlias(ProjectParsed queryData) {
        Map<Selector, String> alias = Collections.emptyMap();
        if (queryData.getSelect() != null) {
            alias = queryData.getSelect().getColumnMap();
        }
        return alias;
    }
}