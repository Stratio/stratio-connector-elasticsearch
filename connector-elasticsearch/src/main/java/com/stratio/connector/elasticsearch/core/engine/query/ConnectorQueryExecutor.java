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

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.commons.util.ColumnTypeHelper;
import com.stratio.connector.commons.util.SelectorHelper;
import com.stratio.connector.elasticsearch.core.engine.metadata.MetadataCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.RowSorter;
import com.stratio.connector.elasticsearch.core.engine.utils.SelectorUtils;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.OrderByClause;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.statements.structures.SelectorType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
            } else if (SelectorUtils.hasFunction(queryData.getSelect().getColumnMap(), "count", "max", "avg", "min", "sum")) {
                processAggregationFucntion(queryData, resultSet, response);
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

    private Object geyBucketValue(Terms.Bucket bucket){
        if (bucket instanceof StringTerms.Bucket){
            return  bucket.getKey();
        }else {

            return bucket.getKeyAsNumber();
        }
    }

    /**
     * Process Group By queries results
     */
    private void processAggregation(ProjectParsed queryData, ResultSet resultSet, SearchResponse response) throws ExecutionException {

        Map<Selector, String> alias = returnAlias(queryData);

        for (Aggregation aggregation : response.getAggregations()) { //TODO support for multiple aggregations
            InternalTerms terms = (InternalTerms) aggregation; //TODO support for different types
            processTermAggregation(queryData, resultSet, alias, terms);
        }

        if (queryData.getOrderBy() != null && !queryData.getOrderBy().getIds().isEmpty()){
            List<OrderByClause> fields = queryData.getOrderBy().getIds();
            Collections.sort(resultSet.getRows(), new RowSorter(fields));
        }

        if (queryData.getLimit() != null){
            int limit = queryData.getLimit().getLimit();
            List<Row> limitedResult = resultSet.getRows().subList(0,limit);
            resultSet.setRows(new ArrayList<>(limitedResult));
        }

    }

    private void processTermAggregation(ProjectParsed queryData, ResultSet resultSet, Map<Selector, String> alias, InternalTerms terms) throws ExecutionException {

        for (Terms.Bucket bucket : terms.getBuckets()) { //Top Level
            Map<String, Object> fields = new HashMap();
            fields.put(terms.getName(), geyBucketValue(bucket)); //First column

            //Has other Aggregations/Columns
            if (bucket.getAggregations().iterator().hasNext()) {
                processSubAggregation(queryData, bucket.getAggregations().asList(), alias, fields, resultSet);
            } else {
//                fields.put("count", bucket.getDocCount());
                resultSet.add(buildRow(queryData, alias, fields));
            }
        }
    }


    private void processSubAggregation(ProjectParsed queryData, List<Aggregation> aggregations, Map<Selector, String> alias, Map<String, Object> fields, ResultSet resultSet) throws ExecutionException {
        for (Aggregation subAgg : aggregations) {
            if (subAgg instanceof InternalTerms){ //Is a Sub Agreggation
                InternalTerms termsSubAgg = (InternalTerms) subAgg;
                for (Terms.Bucket subBucker : termsSubAgg.getBuckets()) {
                    if (subBucker.getAggregations().iterator().hasNext()) {
                        fields.put(termsSubAgg.getName(), geyBucketValue(subBucker));
                        processSubAggregation(queryData, subBucker.getAggregations().asList(), alias, fields, resultSet);
                        resultSet.add(buildRow(queryData, alias, fields));
                    } else {
                        fields.put(termsSubAgg.getName(), geyBucketValue(subBucker));
                        resultSet.add(buildRow(queryData, alias, fields));
                    }
                }
            }else if (subAgg instanceof NumericMetricsAggregation){
                NumericMetricsAggregation.SingleValue numericAggregation = (NumericMetricsAggregation.SingleValue) subAgg;
                fields.put(subAgg.getName(), numericAggregation.value());
                Row row  = buildRow(queryData, alias, fields);
                if (!resultSet.getRows().contains(row)){
                    resultSet.add(row);
                }

            }
        }
    }

    private void processResults(ProjectParsed queryData, ResultSet resultSet, SearchResponse response) throws ExecutionException {
        for (SearchHit hit : response.getHits().getHits()) {
            resultSet.add(createRow(hit, queryData));
        }
    }

    private void processAggregationFucntion(ProjectParsed queryData, ResultSet resultSet, SearchResponse response) throws ExecutionException {

        Map<Selector, String> alias = returnAlias(queryData);
        Map<String, Object> fields = new HashMap();
        for (Aggregation aggregation : response.getAggregations()) { //TODO support for multiple aggregations
            NumericMetricsAggregation.SingleValue numericAggregation = (NumericMetricsAggregation.SingleValue) aggregation;
            fields.put(numericAggregation.getName(), numericAggregation.value());
        }

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
            if (SelectorUtils.isFunction(selector, "count", "avg", "max", "min", "sum")) {
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