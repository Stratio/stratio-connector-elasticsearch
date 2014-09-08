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


import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import com.stratio.connector.elasticsearch.core.engine.utils.LimitModifier;
import com.stratio.connector.meta.ElasticsearchResultSet;
import com.stratio.connector.meta.ICallBack;
import com.stratio.connector.meta.IResultSet;

import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandle;
import com.stratio.connector.elasticsearch.core.exceptions.ElasticsearchQueryException;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.connector.IQueryEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;


public class ElasticsearchQueryEngine implements IQueryEngine {

    ElasticSearchConnectionHandle connectionHandle;

    public ElasticsearchQueryEngine(ElasticSearchConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;

    }

    @Override
    public QueryResult execute(ClusterName targetCluster, LogicalWorkflow workflow) throws ExecutionException, UnsupportedException {
        QueryResult queryResult = null;
        try {
            queryResult = execute(recoveredClient(targetCluster), workflow);
        } catch (UnsupportedOperationException e) {
            e.printStackTrace(); //TODO
        }
        return queryResult;
    }

    private QueryResult execute(Client elasticClient, LogicalWorkflow logicalPlan) throws UnsupportedException, ElasticsearchQueryException, com.stratio.connector.meta.exception.UnsupportedOperationException {

        QueryResult resultSet = null;
        LogicalPlanExecutor executor = new LogicalPlanExecutor(logicalPlan, elasticClient);
        resultSet = executor.executeQuery(elasticClient);
        return resultSet;

    }
    
	//TODO CHECK PROTECTED
    protected Row getRowByID(Client elasticClient, String index, String type, String id){
    	GetResponse response= elasticClient.prepareGet(index, type, id).execute().actionGet();
    	
    	Row row = new Row();
    	
    	if(!response.isSourceEmpty()){
			for (Map.Entry<String, Object> entry : response.getSourceAsMap().entrySet())	{
				row.addCell(entry.getKey(), new Cell(entry.getValue()));
			}
    	}
    	return row;
    	
    }
	


    private Client recoveredClient(ClusterName targetCluster) {
        return (Client) connectionHandle.getConnection(targetCluster.getName()).getClient();
    }
}
