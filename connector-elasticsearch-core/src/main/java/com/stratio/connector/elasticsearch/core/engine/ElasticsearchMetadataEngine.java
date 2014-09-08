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

import com.stratio.connector.elasticsearch.core.connection.ElasticSearchConnectionHandle;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.client.Client;

/**
 * @author darroyo
 *
 */
public class ElasticsearchMetadataEngine implements IMetadataEngine{

	private transient ElasticSearchConnectionHandle connectionHandle;

    public ElasticsearchMetadataEngine(ElasticSearchConnectionHandle connectionHandle) {

        this.connectionHandle = connectionHandle;
    }


    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata) throws UnsupportedException  {
    	//TODO index settings?
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException  {
        //TODO type mappings?
        throw new UnsupportedException("Not yet supported");
    }

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name) throws ExecutionException {
    	//TODO getName o qualifiedName?
    	DeleteIndexResponse delete = recoveredClient(targetCluster).admin().indices().delete(new DeleteIndexRequest(name.getName())).actionGet();
        if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");

    }

    @Override
    public void dropTable(ClusterName targetCluster, TableName name) throws  ExecutionException {
    	 //drop mapping => the table will be deleted
    	//TODO name.getCatalogName() y targetCluster.getName().
        DeleteMappingResponse delete =recoveredClient(targetCluster).admin().indices().prepareDeleteMapping(name.getCatalogName().getName()).setType(name.getName()).execute().actionGet();
        if (!delete.isAcknowledged()) throw new ExecutionException("dropCatalog request has not been acknowledged");
        //TODO configure level??
    }
    
    

    
    private Client recoveredClient(ClusterName targetCluster) {
        return (Client) connectionHandle.getConnection(targetCluster.getName()).getNativeConnection();
    }




//    //TODO fields used in old MetadataEngine (put,get) interface
//    private static final String INDEX = "metadata_storage";
//    private static String TYPE = "default";
//    private ElasticsearchStorageEngine storageEngine = null;
//    private ElasticsearchQueryEngine queryEngine = null;
//    	/* (non-Javadoc)
//	 * @see com.stratio.meta.common.connector.IMetadataEngine#put(java.lang.String, java.lang.String)
//	 */
//	@Override
//	public void put(String key, String metadata) {
//		//TODO validate? key exist? =>key=type y id=1
//		//get then insert?
//
//		Row row = new Row();
//        Map<String, Cell> cells = new HashMap<>();
//        cells.put(key, new Cell(metadata));
//        row.setCells(cells);
//		try {
//			storageEngine.insert(INDEX, TYPE, row, key);
//		} catch (UnsupportedOperationException | ExecutionException e) {
//			// TODO Incluir throws... Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//
//
//	/* (non-Javadoc)
//	 * @see com.stratio.meta.common.connector.IMetadataEngine#get(java.lang.String)
//	 * TODO null if not exist
//	 */
//    @Override
//	public String get(String key) {
//		//TODO getID, getPK
//		Object value;
//		Cell cell;
//		if ((cell= queryEngine.getRowByID(INDEX, TYPE, key).getCell(key)) != null){
//			if ((value =cell.getValue()) != null){
//				return (value instanceof String) ? (String) value : null;
//			} else return null;
//		}else return null;
//
//	}
//
//		/**
//	 * @param elasticStorageEngine
//	 */
//    public void setStorageEngine(ElasticsearchStorageEngine elasticStorageEngine) {
//        storageEngine = elasticStorageEngine;
//
//    }
//
//    /**
//     * @param elasticQueryEngine
//     */
//    public void setQueryEngine(ElasticsearchQueryEngine elasticQueryEngine) {
//        queryEngine  = elasticQueryEngine;
//
//    }
}

