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
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.CatalogMetadata;
import com.stratio.meta2.common.metadata.TableMetadata;

/**
 * @author darroyo
 */
public class ElasticsearchMetadataEngine implements IMetadataEngine {

    private static final String INDEX = "metadata_storage";
    private static String TYPE = "default";
    private final ElasticSearchConnectionHandle connectionHandle;


    private ElasticsearchStorageEngine storageEngine = null;
    private ElasticsearchQueryEngine queryEngine = null;

    public ElasticsearchMetadataEngine(ElasticSearchConnectionHandle connectionHandle) {
        this.connectionHandle = connectionHandle;
    }

	
	
	

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IMetadataEngine#put(java.lang.String, java.lang.String)
	 * /
	@Override
	public void put(String key, String metadata) {
		//TODO validate? key exist? =>key=type y id=1
		//get then insert?
		
		Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(key, new Cell(metadata));
        row.setCells(cells);
		try {
			storageEngine.insert(INDEX, TYPE, row);
		} catch (UnsupportedOperationException | ExecutionException e) {
			// TODO Incluir throws... Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/* (non-Javadoc)
	 * @see com.stratio.meta.common.connector.IMetadataEngine#get(java.lang.String)
	 * /
	@Override
	public String get(String key) {
		//TODO getID, getPK

		List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = Arrays.asList(new ColumnMetadata(TYPE,key));
        Project project = new Project(INDEX, TYPE,columns);
        stepList.add(project);
		LogicalPlan logicalPlan = new LogicalPlan(stepList);
		
		
        QueryResult queryResult = null;
		try {
			queryResult = (QueryResult) queryEngine.execute(logicalPlan);
		} catch (UnsupportedOperationException | ExecutionException
				| UnsupportedException e) {
			// TODO throws Auto-generated catch block
			e.printStackTrace();
		}
        
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        Row row = null;
        while(rowIterator.hasNext()){
        	if(row != null) {
        		row = null; break;//TODO throw new Exception
        	}else row = rowIterator.next();
        	
        }
        
        return (String) row.getCells().get(key).getValue();
        
	} */

    /**
     * @param elasticStorageEngine
     */
    public void setStorageEngine(ElasticsearchStorageEngine elasticStorageEngine) {
        storageEngine = elasticStorageEngine;

    }

    /**
     * @param elasticQueryEngine
     */
    public void setQueryEngine(ElasticsearchQueryEngine elasticQueryEngine) {
        queryEngine = elasticQueryEngine;

    }


    //REVIEW Esto es la nueva interfaz, lo anterior estaba de antes hay que revisarlos.


    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata) throws UnsupportedException, ExecutionException {

    }

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata) throws UnsupportedException, ExecutionException {

    }

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name) throws UnsupportedException, ExecutionException {

    }

    @Override
    public void dropTable(ClusterName targetCluster, TableName name) throws UnsupportedException, ExecutionException {

    }
}
