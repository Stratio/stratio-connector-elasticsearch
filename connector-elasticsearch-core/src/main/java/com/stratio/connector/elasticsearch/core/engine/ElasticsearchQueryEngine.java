/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.engine;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

import com.stratio.connector.commons.connection.Connection;
import com.stratio.connector.commons.connection.ConnectionHandler;
import com.stratio.connector.commons.engine.UniqueProjectQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryParser;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.result.QueryResult;

public class ElasticsearchQueryEngine extends UniqueProjectQueryEngine<Client> {

    private ConnectorQueryParser queryParser = new ConnectorQueryParser();
    private ConnectorQueryBuilder queryBuilder = new ConnectorQueryBuilder();
    private ConnectorQueryExecutor queryExecutor = new ConnectorQueryExecutor();

    public ElasticsearchQueryEngine(ConnectionHandler connectionHandle) {

        super(connectionHandle);

    }

    @Override
    protected QueryResult execute(Project logicalWorkflow, Connection<Client> connection)
            throws UnsupportedException, ExecutionException {

        Client elasticClient = connection.getNativeConnection();
        ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(logicalWorkflow);

        SearchRequestBuilder requestBuilder = queryBuilder.buildQuery(elasticClient, queryData);

        return queryExecutor.executeQuery(elasticClient, requestBuilder, queryData);

    }

    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Async query not supported in ElasticSearch");

    }

    @Override
    public void stop(String queryId) throws UnsupportedException, ExecutionException {
        throw new UnsupportedException("Stop query query not supported in ElasticSearch");
    }
}
    

