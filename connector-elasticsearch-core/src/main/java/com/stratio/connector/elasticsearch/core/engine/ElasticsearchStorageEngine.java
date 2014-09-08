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

import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandle;
import com.stratio.connector.elasticsearch.core.engine.utils.FilterBuilderHelper;
import com.stratio.connector.elasticsearch.core.exceptions.ElasticsearchDeleteException;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.connector.IStorageEngine;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
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

import java.util.*;


/**
 * This class performs operations insert and delete in Elasticsearch.
 */

public class ElasticsearchStorageEngine implements IStorageEngine {


    private transient ElasticSearchConnectionHandle connectionHandle;


    public ElasticsearchStorageEngine(ElasticSearchConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;
    }


    @Override
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Row row) throws UnsupportedException, ExecutionException {

        insert((Client)connectionHandle.getConnection(targetCluster.getName()).getNativeConnection(), targetTable, row);

    }


    @Override
    public void insert(ClusterName targetCluster, TableMetadata targetTable, Collection<Row> rows ) throws UnsupportedException, ExecutionException {
        insert((Client)connectionHandle.getConnection(targetCluster.getName()).getNativeConnection(), targetTable, rows);
    }




    /**
     * Insert a document in Elasticsearch.
     *
     * @param targetTable the targetName.
     * @param row         the row.
     * @throws ExecutionException in case of failure during the execution.
     */

    private void insert(Client client, TableMetadata targetTable, Row row)
            throws ExecutionException, UnsupportedException {
        //TODO connector should check??
        String index = targetTable.getName().getCatalogName().getName();
        String type = targetTable.getName().getName();
        if (isEmpty(index) || isEmpty(type) || row == null) {
            throw new ExecutionException("illegal insert: index, type and row cannot be empties");
        } else {
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
            createIndexRequestBuilder(targetTable,client, index, type, row).execute().actionGet();

        }

    }


    /**
     * Insert a set of documents in Elasticsearch.
     *
     * @param targetTable the target table.
     * @param rows        the set of rows.
     * @throws ExecutionException in case of failure during the execution.
     */
    private void insert(Client elasticClient, TableMetadata targetTable, Collection<Row> rows)
            throws UnsupportedException, ExecutionException {
        String index = targetTable.getName().getCatalogName().getName();
        String type = targetTable.getName().getName();
//		_id required	
//		if (isEmpty(catalog) || isEmpty(tableName) || rows == null || rows.isEmpty()) {
//			//throwException
//		}else{
	
    BulkRequestBuilder bulkRequest = elasticClient.prepareBulk();

    for (Row row : rows) {
        bulkRequest.add(createIndexRequestBuilder(targetTable,elasticClient, index, type, row));
    }

    BulkResponse bulkResponse = bulkRequest.execute().actionGet();

    if (bulkResponse.hasFailures()) throw new ExecutionException(bulkResponse.buildFailureMessage());

}

	
	   /**
     * Returns an IndexRequestBuilder. Adds the json created from the row
     */
    private IndexRequestBuilder createIndexRequestBuilder(TableMetadata targetTable, Client elasticClient, String index, String type, Row row) throws UnsupportedException {


        Map<String, Object> json = new HashMap<String, Object>();
        String pk = null;
        IndexRequestBuilder indexRequestBuilder = null;
        for (Map.Entry<String, Cell> entry : row.getCells().entrySet()) {
            Object cellValue = entry.getValue().getValue();
            //TODO check if the cell is primaryKey?
            json.put(entry.getKey(), cellValue);
            if (targetTable.isPK(new ColumnName(targetTable.getName().getCatalogName().getName(),targetTable.getName().getName(), entry.getKey()))){
                if (pk!=null) throw new UnsupportedException("Only one PK is allowed");
                pk = entry.getValue().getValue().toString(); //TODO revisar el toString.
            }
        }
        if (pk!=null){
            indexRequestBuilder =  elasticClient.prepareIndex(index,type,pk).setSource(json);
        }else {
            indexRequestBuilder =elasticClient.prepareIndex(index, type).setSource(json);
        }
        return  indexRequestBuilder;


    }
    

			

    /**
     * Delete a set of documents.
     *
     * @param index   the index.
     * @param type    the type.
     * @param filters filters to restrict the set of documents.
     */

    private void delete(Client elasticClient, String index, String type, Filter... filters)
            throws UnsupportedOperationException, ElasticsearchDeleteException {

        DeleteByQueryResponse response;

        if (filters == null || filters.length == 0) {
            response = elasticClient.prepareDeleteByQuery(index).setTypes(type)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .execute().actionGet();
        } else {

            //TODO if filters contains a token condition check with FILTERHELPER => response= elasticClient.prepareDelete(catalog, tableName, id);

            FilterBuilder filterBuilder = FilterBuilderHelper.createFilterBuilder(new ArrayList<Filter>(Arrays.asList(filters)));

            DeleteByQueryRequestBuilder deleteRequestBuilder = elasticClient.prepareDeleteByQuery(index)
                    .setTypes(type).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder));

            response = deleteRequestBuilder.execute().actionGet();

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

	
	/**
     * Insert a document in Elasticsearch.
     *
     * @param index			the database.
     * @param type		    the type.
     * @param row      		the row.
     * @param id			the id 
     * @throws ExecutionException  in case of failure during the execution.
     */
	
	protected void insert(Client elasticClient, String index, String type, Row row, String id) throws UnsupportedOperationException, ExecutionException {
		//TODO if (null) => ... public insert... should call this method
		
		//TODO connector should check??
		if (isEmpty(index) || isEmpty(type) || row == null || isEmpty(id)) {
			throw new ExecutionException("illegal insert: index, type and row cannot be empties"); 
		}else{
			//CHECK BEFORE INSERTING?			
			//TODO read configuration to set index settings
			try {
				IndexResponse response = createIndexRequestBuilder(elasticClient,index, type, row,id).execute().get();
			} catch (InterruptedException | java.util.concurrent.ExecutionException e) {
				throw new ExecutionException("Data has not been indexed");

			}
			//check response
		}
			
	}
	
	/**
	 * Returns an IndexRequestBuilder. Adds the json created from the row
	 */
	private IndexRequestBuilder createIndexRequestBuilder(Client elasticClient, String index, String type, Row row,String id){
		
	
		Map<String, Object> json = new HashMap<String,Object>();
		for (Map.Entry<String, Cell> entry : row.getCells().entrySet()){
			Object cellValue = entry.getValue().getValue();	
			//TODO check if the cell is primaryKey?
			json.put(entry.getKey(), cellValue);
		}
		return elasticClient.prepareIndex(index,type,id).setSource(json);	
		
		
	}

    private void validateDelete(DeleteByQueryResponse response) throws ElasticsearchDeleteException {

        Iterator<IndexDeleteByQueryResponse> iter = response.iterator();
        IndexDeleteByQueryResponse indexDeleteResponse;
        while (iter.hasNext()) {
            indexDeleteResponse = iter.next();
            if (indexDeleteResponse.getFailedShards() > 0) {
                throw new ElasticsearchDeleteException("#failed Shards: " + indexDeleteResponse.getFailedShards());
            }
        }

    }


    private boolean isEmpty(String value) {
        return value == null || value.trim().isEmpty();
    }
}



