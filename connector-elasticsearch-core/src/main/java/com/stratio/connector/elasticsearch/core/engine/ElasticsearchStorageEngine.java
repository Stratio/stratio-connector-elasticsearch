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

import java.util.*;

import com.stratio.connector.elasticsearch.core.connection.ConnectionHandle;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import com.stratio.connector.elasticsearch.core.engine.utils.FilterBuilderHelper;
import com.stratio.connector.elasticsearch.core.exceptions.ElasticsearchDeleteException;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.Filter;



/**
 * This class performs operations insert and delete in Elasticsearch.
 */

public class ElasticsearchStorageEngine implements IStorageEngine {



   private transient ConnectionHandle connectionHandle;


    public ElasticsearchStorageEngine(ConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;
    }


    @Override
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row) throws UnsupportedException, ExecutionException {

        insert(recoveredClient(targetCluster), targetTable, row);

    }





     @Override
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows) throws UnsupportedException, ExecutionException {
        insert(recoveredClient(targetCluster), targetTable, rows);
    }

    private Client recoveredClient(ClusterName targetCluster) {
        return (Client) connectionHandle.getConnection(targetCluster.getName()).getClient();
    }


    /**
     * Insert a document in Elasticsearch.
     *
     * @param targetTable the targetName.
     *
     * @param row      		the row.
     * @throws ExecutionException  in case of failure during the execution.
     */

    private void insert(Client client, TableMetadata targetTable, Row row)
			throws ExecutionException {
		//TODO connector should check??
        String index = targetTable.getName().getCatalogName().getName();
        String type = targetTable.getName().getName();
		if (isEmpty(index) || isEmpty(type) || row == null) {
			throw new ExecutionException("illegal insert: index, type and row cannot be empties"); 
		}else{
//				CHECK BEFORE INSERTING?			
//							if(cellValue instanceof Integer || 
//							cellValue instanceof Pattern || 
//							...
//							cellValue instanceof Date || 
//							cellValue instanceof DBRef || 
//							cellValue instanceof Binary || 
//							cellValue instanceof byte[] || 
//							cellValue instanceof Boolean || 
//							
//							){
//					}
			//TODO read configuration to set index settings
			createIndexRequestBuilder(client,index, type, row).execute().actionGet();

        }
			
	}

	
	
	/**
     * Insert a set of documents in Elasticsearch.
     *
     * @param targetTable			the target table.
     * @param rows      	the set of rows.
     * @throws ExecutionException  in case of failure during the execution.
     */
    private void insert(Client elasticClient, TableMetadata targetTable,  Collection<Row> rows)
			throws UnsupportedException, ExecutionException {
        String index = targetTable.getName().getCatalogName().getName();
        String type = targetTable.getName().getName();
//		_id required	
//		if (isEmpty(catalog) || isEmpty(tableName) || rows == null || rows.isEmpty()) {
//			//throwException
//		}else{
		
		BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();
		
		for(Row row: rows){
			bulkRequest.add(createIndexRequestBuilder(elasticClient,index, type, row));
		}
		
		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		
		if( bulkResponse.hasFailures()) throw new ExecutionException(bulkResponse.buildFailureMessage());	
								
	}

	/**
	 * Returns an IndexRequestBuilder. Adds the json created from the row
	 */
	private IndexRequestBuilder createIndexRequestBuilder(Client elasticClient, String index, String type, Row row){
		
		
		Map<String, Object> json = new HashMap<String,Object>();
		for (Map.Entry<String, Cell> entry : row.getCells().entrySet()){
			Object cellValue = entry.getValue().getValue();	
			//TODO check if the cell is primaryKey?
			json.put(entry.getKey(), cellValue);
		}
		return elasticClient.prepareIndex(index,type).setSource(json);	
		
		
	}
	
	/** Delete a set of documents.
	*
	* @param index 		the index.
	* @param type 		the type.
	* @param filters 	filters to restrict the set of documents.
	*/
	
	private void delete(Client elasticClient, String index, String type, Filter... filters)
			throws UnsupportedOperationException, ElasticsearchDeleteException {
		
		DeleteByQueryResponse response;
		
		if (filters == null || filters.length == 0){
			response= elasticClient.prepareDeleteByQuery(index).setTypes(type)
					.setQuery(QueryBuilders.matchAllQuery())
					.execute().actionGet();
		}else{
			
			//TODO if filters contains a token condition check with FILTERHELPER => response= elasticClient.prepareDelete(catalog, tableName, id);
			
			FilterBuilder filterBuilder= FilterBuilderHelper.createFilterBuilder(new ArrayList<Filter>(Arrays.asList(filters)));
			
			DeleteByQueryRequestBuilder deleteRequestBuilder = elasticClient.prepareDeleteByQuery(index)
					.setTypes(type).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder));
			
			response=  deleteRequestBuilder.execute().actionGet();
			
			/*
//			System.out.println("S.O DEBUG"+ boolFilterBuilder.toString());
//					.setConsistencyLevel(null)
//					.setIndicesOptions(null)
//					.setReplicationType("")
//					.setRouting("")
//					.setTimeout("")			
			*/
		}
		
		validateDelete(response);
		


	}
	
	
	private void validateDelete(DeleteByQueryResponse response) throws ElasticsearchDeleteException {
		
		Iterator<IndexDeleteByQueryResponse> iter = response.iterator();
		IndexDeleteByQueryResponse indexDeleteResponse;
		while(iter.hasNext()){
			indexDeleteResponse = iter.next();
			if (indexDeleteResponse.getFailedShards()>0){
				throw new ElasticsearchDeleteException("#failed Shards: "+indexDeleteResponse.getFailedShards());
			}
		}
		
	}


	private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
}



