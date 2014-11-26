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

package com.stratio.connector.elasticsearch.core.engine;

import java.util.Collection;
import java.util.Collections;

import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.elasticsearch.core.engine.utils.IndexRequestBuilderCreator;
import com.stratio.connector.elasticsearch.core.engine.utils.QueryBuilderCreator;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.TableMetadata;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.RelationSelector;

/**
 * This class performs operations insert and delete in Elasticsearch.
 */

public class ElasticsearchStorageEngine extends CommonsStorageEngine<Client> {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    /**
     * The index creator builder.
     */
    private IndexRequestBuilderCreator indexRequestBuilderCreator = new IndexRequestBuilderCreator();

    /**
     * Constructor.
     *
     * @param connectionHandler the connection handler.
     */
    public ElasticsearchStorageEngine(ConnectionHandler connectionHandler) {

        super(connectionHandler);
    }

    @Override protected void truncate(TableName tableName, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        delete(tableName, Collections.EMPTY_LIST, connection);

    }

    @Override protected void delete(TableName tableName, Collection<Filter> whereClauses, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {
        String index = tableName.getCatalogName().getName();

        QueryBuilderCreator queryBuilderCreator = new QueryBuilderCreator();


        connection.getNativeConnection().prepareDeleteByQuery(index).setQuery(queryBuilderCreator.createBuilder
                (whereClauses)).execute().actionGet();

    }

    @Override protected void update(TableName tableName, Collection<Relation> assignments,
            Collection<Filter> whereClauses, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {

        String index = tableName.getCatalogName().getName();
        String type = tableName.getName();
        QueryBuilderCreator queryBuilderCreator = new QueryBuilderCreator();
        for(Relation relation: assignments) {
            FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(queryBuilderCreator.createBuilder(whereClauses));
            CombineFunction combineFunction = chooseCombineFunction(relation);
            functionScoreQueryBuilder.boostMode(combineFunction);
            FilterBuilder scorefuntion;

            functionScoreQueryBuilder.add(ScoreFunctionBuilders.scriptFunction("name = 'hola'"));
            connection.getNativeConnection().prepareSearch(index).setTypes(type).setQuery(functionScoreQueryBuilder);
        }
        //throw new UnsupportedException("Not yet supported"); //TODO
    }


    private CombineFunction chooseCombineFunction(Relation relation) throws UnsupportedException {
        CombineFunction combineFunction = null;
        if (!(relation.getRightTerm() instanceof RelationSelector)) {
            //TODO
        }
        RelationSelector rightSelect = (RelationSelector)relation.getRightTerm();
        switch (rightSelect.getRelation().getOperator()){
        case ADD:
        case SUBTRACT:
            combineFunction= CombineFunction.SUM;
            break;
        case DIVISION:
        case MULTIPLICATION: combineFunction = CombineFunction.MULT;break;
        default: throw new UnsupportedException("The operator "+relation.getOperator()+" is not supported for " +
                "update");
        }
        return combineFunction;
    }


    /**
     * Insert a document in Elasticsearch.
     *
     * @param targetTable the targetName.
     * @param row         the row.
     * @throws ExecutionException   in case of failure during the execution.
     * @throws UnsupportedException it the operation is not supported.
     */

    @Override
    protected void insert(TableMetadata targetTable, Row row, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {

        try {

            IndexRequestBuilder indexRequestBuilder = createIndexRequest(targetTable, row, connection);
            indexRequestBuilder.execute().actionGet();

            loggInsert(targetTable);

        } catch (HandlerConnectionException e) {

            throwHandlerException(e, "insert");

        }

    }

    /**
     * Insert a set of documents in Elasticsearch.
     *
     * @param rows the set of rows.
     * @throws ExecutionException   in case of failure during the execution.
     * @throws UnsupportedException if the operation is not supported.
     */
    protected void insert(TableMetadata targetTable, Collection<Row> rows,
            Connection<Client> connection) throws UnsupportedException, ExecutionException {

        try {
            BulkRequestBuilder bulkRequest = createBulkRequest(targetTable, rows, connection);

            BulkResponse bulkResponse = bulkRequest.execute().actionGet();

            validateBulkResponse(bulkResponse);

            logBulkInsert(targetTable, rows);

        } catch (HandlerConnectionException e) {
            throwHandlerException(e, "insert bulk");
        }

    }

    /**
     * This method creates a IndexRequestBuilder.
     *
     * @param tableMetadata the table metadata.
     * @param row           the row to insert.
     * @param connection    the logical connection.
     * @return the index request builder.
     * @throws HandlerConnectionException if a exceptions occurs during connection handling.
     * @throws UnsupportedException       if a operation is not supported.
     */
    private IndexRequestBuilder createIndexRequest(TableMetadata tableMetadata, Row row,
            Connection<Client> connection) throws HandlerConnectionException, UnsupportedException {

        Client client = connection.getNativeConnection();

        return indexRequestBuilderCreator.createIndexRequestBuilder(tableMetadata, client, row);
    }

    /**
     * This method create a bulkRequestBuilder.
     *
     * @param tablesMetadata the table metadata
     * @param rows           the rows to insert.
     * @param connection     the logical connection.
     * @return the index request builder.
     * @throws HandlerConnectionException if a exceptions occurs during connection handling.
     * @throws UnsupportedException       if a operation is not supported.
     */
    private BulkRequestBuilder createBulkRequest(TableMetadata tablesMetadata,
            Collection<Row> rows, Connection<Client> connection)
            throws HandlerConnectionException, UnsupportedException {

        Client elasticClient = connection.getNativeConnection();

        BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();

        for (Row row : rows) {
            IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator
                    .createIndexRequestBuilder(tablesMetadata, elasticClient, row);
            bulkRequest.add(indexRequestBuilder);
        }
        return bulkRequest;
    }

    /**
     * Check if the bulk insert has been correct.
     *
     * @param bulkResponse the bulk response.
     * @throws ExecutionException if an error happens during the execution.
     */
    private void validateBulkResponse(BulkResponse bulkResponse) throws ExecutionException {
        if (bulkResponse.hasFailures()) {
            throw new ExecutionException(bulkResponse.buildFailureMessage());
        }
    }

    /**
     * Log the insert.
     *
     * @param tableMetadata the table metadata.
     */
    private void loggInsert(TableMetadata tableMetadata) {
        if (logger.isDebugEnabled()) {
            String index = tableMetadata.getName().getCatalogName().getName();
            String type = tableMetadata.getName().getName();
            logger.debug("Insert one row in ElasticSearch Database. Index [" + index + "] Type [" + type + "]");
        }
    }

    /**
     * Log the bulf insert.
     *
     * @param tableMetadata the table metadata.
     * @param rows          the rows.
     */
    private void logBulkInsert(TableMetadata tableMetadata, Collection<Row> rows) {
        if (logger.isDebugEnabled()) {
            String index = tableMetadata.getName().getCatalogName().getName();
            String type = tableMetadata.getName().getName();
            logger.debug(
                    "Insert " + rows.size() + "  rows in ElasticSearch Database. Index [" + index + "] Type [" + type
                            + "]");
        }
    }

    /**
     * Throw a handlerExcepcion.
     *
     * @param e      original exception.
     * @param method the method which throw the exception.
     * @throws ExecutionException
     */
    private void throwHandlerException(HandlerConnectionException e, String method) throws ExecutionException {
        String exceptionMessage = "Fail Connecting elasticSearch in " + method + " method. " + e.getMessage();
        logger.error(exceptionMessage);
        throw new ExecutionException(exceptionMessage, e);
    }

}



