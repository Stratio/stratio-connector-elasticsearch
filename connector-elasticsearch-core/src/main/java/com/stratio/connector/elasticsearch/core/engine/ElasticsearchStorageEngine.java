/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.elasticsearch.core.engine;

import java.util.Collection;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.commons.engine.CommonsStorageEngine;
import com.stratio.connector.elasticsearch.core.engine.utils.IndexRequestBuilderCreator;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.TableMetadata;

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

    private IndexRequestBuilder createIndexRequest(TableMetadata targetTable, Row row,
            Connection<Client> connection) throws HandlerConnectionException, UnsupportedException {

        Client client = connection.getNativeConnection();

        return indexRequestBuilderCreator.createIndexRequestBuilder(targetTable, client, row);
    }

    private BulkRequestBuilder createBulkRequest(TableMetadata targetTable,
            Collection<Row> rows, Connection<Client> connection)
            throws HandlerConnectionException, UnsupportedException {

        Client elasticClient = connection.getNativeConnection();

        BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();

        int i = 0;
        for (Row row : rows) {
            IndexRequestBuilder indexRequestBuilder = indexRequestBuilderCreator
                    .createIndexRequestBuilder(targetTable, elasticClient, row);
            bulkRequest.add(indexRequestBuilder);
            ;
        }
        return bulkRequest;
    }

    private void validateBulkResponse(BulkResponse bulkResponse) throws ExecutionException {
        if (bulkResponse.hasFailures()) {
            throw new ExecutionException(bulkResponse.buildFailureMessage());
        }
    }

    private void loggInsert(TableMetadata targetTable) {
        if (logger.isDebugEnabled()) {
            String index = targetTable.getName().getCatalogName().getName();
            String type = targetTable.getName().getName();
            logger.debug("Insert one row in ElasticSearch Database. Index [" + index + "] Type [" + type + "]");
        }
    }

    private void logBulkInsert(TableMetadata targetTable, Collection<Row> rows) {
        if (logger.isDebugEnabled()) {
            String index = targetTable.getName().getCatalogName().getName();
            String type = targetTable.getName().getName();
            logger.debug(
                    "Insert " + rows.size() + "  rows in ElasticSearch Database. Index [" + index + "] Type [" + type
                            + "]");
        }
    }

    private void throwHandlerException(HandlerConnectionException e, String method) throws ExecutionException {
        String exceptionMessage = "Fail Connecting elasticSearch in " + method + " method. " + e.getMessage();
        logger.error(exceptionMessage);
        throw new ExecutionException(exceptionMessage, e);
    }

}



