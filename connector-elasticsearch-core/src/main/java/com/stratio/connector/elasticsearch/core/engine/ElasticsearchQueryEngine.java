/**
* Copyright (C) 2014 Stratio (http://stratio.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.stratio.connector.elasticsearch.core.engine;

import com.stratio.connector.elasticsearch.core.connection.ConnectionHandle;
import com.stratio.connector.elasticsearch.core.exceptions.ElasticsearchQueryException;
import com.stratio.connector.meta.exception.*;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta2.common.data.ClusterName;
import org.elasticsearch.client.Client;

import com.stratio.connector.meta.ElasticsearchResultSet;
import com.stratio.connector.meta.ICallBack;
import com.stratio.connector.meta.IResultSet;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.result.QueryResult;




public class ElasticsearchQueryEngine implements IQueryEngine {

    ConnectionHandle connectionHandle;

    public ElasticsearchQueryEngine(ConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;

    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws ExecutionException, UnsupportedException {
        QueryResult queryResult = null;
        try {
            queryResult =  execute(recoveredClient(targetCluster),workflow);
        } catch (UnsupportedOperationException e) {
            e.printStackTrace(); //TODO
        }
        return queryResult;
    }

    private QueryResult execute(Client elasticClient,LogicalWorkflow logicalPlan) throws UnsupportedException, ElasticsearchQueryException, com.stratio.connector.meta.exception.UnsupportedOperationException {
    	
    	QueryResult resultSet =null;
    	LogicalPlanExecutor executor = new LogicalPlanExecutor(logicalPlan, elasticClient);
    	resultSet = executor.executeQuery(elasticClient);
    	return resultSet; 	

    }


    private Client recoveredClient(ClusterName targetCluster) {
        return (Client) connectionHandle.getConnection(targetCluster.getName()).getClient();
    }
}
