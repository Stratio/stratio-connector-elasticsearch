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
package com.stratio.connector.elasticsearch.ftest.functionalMetadata;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;

public class DropTest extends ConnectionTest {

	@Test
	public void dropCollectionTest() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException, UnsupportedException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

        TableMetadata targetTable = new TableMetadata(new TableName(CATALOG,COLLECTION),null,null,null,null,null);
		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME, new TableMetadata(new TableName(CATALOG,COLLECTION),null,null,null,null,null), row);
		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME, new TableMetadata(new TableName(CATALOG+"b",COLLECTION),null,null,null,null,null), row);
		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME,  new TableMetadata(new TableName(CATALOG,"Collect"),null,null,null,null,null), row);


        refresh(CATALOG);
        refresh(CATALOG+"b");
		
		((IMetadataProvider) stratioElasticConnector.getMedatadaProvider()).dropTable(CATALOG, COLLECTION);


		assertEquals("Collection deleted", false, nodeClient.admin().indices().typesExists(new TypesExistsRequest(new String[]{CATALOG}, COLLECTION)).actionGet().isExists() );
		
		assertEquals( true, nodeClient.admin().indices().typesExists(new TypesExistsRequest(new String[]{CATALOG+"b"}, COLLECTION)).actionGet().isExists() );
		
		nodeClient.admin().indices().delete(new DeleteIndexRequest(CATALOG+"b")).actionGet();
	}
	
	@Test
	public void dropCatalogTest() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException, UnsupportedException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME, new TableMetadata(new TableName(CATALOG,"Collect"),null,null,null,null,null), row);

        refresh(CATALOG);

        assertEquals("Catalog deleted", true, nodeClient.admin().indices().exists(new IndicesExistsRequest(CATALOG)).actionGet().isExists());
		
		((IMetadataProvider) stratioElasticConnector.getMedatadaProvider()).dropCatalog(CATALOG);

        refresh(CATALOG);

        assertEquals("Catalog deleted", false, nodeClient.admin().indices().exists(new IndicesExistsRequest(CATALOG)).actionGet().isExists());

	}
	
	
}