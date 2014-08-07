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

    private Client elasticClient = null;

    public QueryResult execute(LogicalPlan logicalPlan) throws com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException, UnsupportedException {
    	
    	QueryResult resultSet =null;
    	LogicalPlanExecutor executor = new LogicalPlanExecutor(logicalPlan, elasticClient);
    	resultSet = executor.executeQuery();  		
    	return resultSet; 	

    }

    public IResultSet execute(IResultSet previousResult, LogicalPlan logicalPlan) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    public IResultSet execute(LogicalPlan logicalPlan, ICallBack callback) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not yet supported");

    }
    

	
	  /**
* Set the connection.
* @param elasticsearchClient the connection.
*/
    public void setConnection(Client elasticClient) {
        this.elasticClient = elasticClient;
    }
}
