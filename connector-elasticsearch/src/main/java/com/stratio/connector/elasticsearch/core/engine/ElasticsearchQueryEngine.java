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

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.SingleProjectQueryEngine;
import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.elasticsearch.core.engine.query.ESProjectParsedValidator;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

/**
 * This class is the responsible of manage the ElasticSearchMetadata.
 */
public class ElasticsearchQueryEngine extends SingleProjectQueryEngine<Client> {

    private ConnectorQueryBuilder queryBuilder = new ConnectorQueryBuilder();
    private ConnectorQueryExecutor queryExecutor = new ConnectorQueryExecutor();

    /**
     * Instantiates a new elasticsearch query engine.
     *
     * @param connectionHandler the connection handler
     */
    public ElasticsearchQueryEngine(ConnectionHandler connectionHandler) {

        super(connectionHandler);

    }

    @Override
    protected void pagedExecute(String queryId, Project project, Connection connection, IResultHandler resultHandler) throws ConnectorException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected void asyncExecute(String queryId, Project project, Connection connection, IResultHandler resultHandler) throws ConnectorException {
        throw new UnsupportedException("Not supported");
    }

    @Override
    protected QueryResult execute(Project project, Connection<Client> connection) throws ConnectorException {
        Client elasticClient = connection.getNativeConnection();
        ProjectParsed projectParsed = new ProjectParsed(project, new ESProjectParsedValidator());
        SearchRequestBuilder requestBuilder = queryBuilder.buildQuery(elasticClient, projectParsed);

        return queryExecutor.executeQuery(elasticClient, requestBuilder, projectParsed);
    }


    @Override
    public void stop(String queryId) throws UnsupportedException {
        throw new UnsupportedException("Not supported");

    }


}
