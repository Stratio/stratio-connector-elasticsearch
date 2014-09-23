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
package com.stratio.connector.elasticsearch.core.engine;


import com.stratio.connector.commons.connection.exceptions.HandlerConnectionException;
import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandler;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryBuilder;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryData;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryExecutor;
import com.stratio.connector.elasticsearch.core.engine.query.ConnectorQueryParser;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ElasticsearchQueryEngine implements IQueryEngine {


    /**
     * The log.
     */
    final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The connection handler.
     */
    private ElasticSearchConnectionHandler connectionHandle;


    public ElasticsearchQueryEngine(ElasticSearchConnectionHandler connectionHandle) {

        this.connectionHandle = connectionHandle;

    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws ExecutionException, UnsupportedException {
        QueryResult queryResult = null;


        try {
            queryResult = execute((Client) connectionHandle.getConnection(targetCluster.getName()).getNativeConnection(), workflow);

        } catch (HandlerConnectionException e) {
            String msg = "Error recovered ElasticSearch connection. "+e.getMessage();
            logger.error(msg);
            throw new ExecutionException(msg,e);
        }
        return queryResult;
    }

    private QueryResult execute(Client elasticClient, LogicalWorkflow logicalWorkFlow) throws UnsupportedException, ExecutionException {


        ConnectorQueryParser queryParser = new ConnectorQueryParser();
        ConnectorQueryData queryData = queryParser.transformLogicalWorkFlow(logicalWorkFlow);
        ConnectorQueryBuilder queryBuilder = new ConnectorQueryBuilder();
        SearchRequestBuilder requestBuilder = queryBuilder.buildQuery(elasticClient, queryData);
        ConnectorQueryExecutor queryExecutor = new ConnectorQueryExecutor();
        QueryResult resultSet = queryExecutor.executeQuery(elasticClient, requestBuilder, queryData);


        return resultSet;

    }


}
