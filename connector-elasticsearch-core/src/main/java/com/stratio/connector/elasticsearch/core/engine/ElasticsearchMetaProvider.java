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
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;

import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.exceptions.ExecutionException;

/**
* This class represents a MetaInfo Provider for Elasticsearch.
*/

public class ElasticsearchMetaProvider implements IMetadataProvider {
	 /**
	* The connection.
	*/
	private Client elasticClient = null;

   private transient ConnectionHandle connectionHandle;

    public ElasticsearchMetaProvider(ConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;
    }


    @Override
    public void createCatalog(String catalog) throws UnsupportedOperationException {
    	//TODO index settings?
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public void createTable(String catalog, String table) throws UnsupportedOperationException {
    	//TODO type mappings?
        throw new UnsupportedOperationException("Not yet supported");
    }

    @Override
    public void dropCatalog(String catalog) throws UnsupportedOperationException, ExecutionException {
    	DeleteIndexResponse delete = elasticClient.admin().indices().delete(new DeleteIndexRequest(catalog)).actionGet();
    			if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");
    			
    }

 
    @Override
    public void dropTable(String catalog, String table) throws UnsupportedOperationException, ExecutionException {
    	//drop mapping => the table will be deleted	
    	DeleteMappingResponse delete = elasticClient.admin().indices().prepareDeleteMapping(catalog).setType(table).execute().actionGet();
    	
		if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");
		//TODO configure level??
		
    }
	

	@Override
	public void createIndex(String catalog, String tableName, String... fields) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("Not supported");
		
	}

	@Override
	public void dropIndex(String catalog, String tableName, String... fields) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("not supported");
		
	}

	@Override
	public void dropIndexes(String catalog, String tableName) throws UnsupportedOperationException {
		throw new UnsupportedOperationException("not supported");
		
	}
		

		
	
}
