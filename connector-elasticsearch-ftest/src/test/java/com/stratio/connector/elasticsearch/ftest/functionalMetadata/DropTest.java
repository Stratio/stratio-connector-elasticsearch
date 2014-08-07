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

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.common.util.ArrayUtils;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.IMetadataProvider;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;

public class DropTest extends ConnectionTest {

	@Test
	public void dropCollectionTest() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG+"b", COLLECTION, row);
		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG, "Collect", row);
		
		
		refresh();
		refresh(CATALOG+"b");
		
		((IMetadataProvider) stratioElasticConnector.getMedatadaProvider()).dropTable(CATALOG, COLLECTION);


		assertEquals("Collection deleted", false, client.admin().indices().typesExists(new TypesExistsRequest(new String[]{CATALOG}, COLLECTION)).actionGet().isExists() ); 
		
		assertEquals( true, client.admin().indices().typesExists(new TypesExistsRequest(new String[]{CATALOG+"b"}, COLLECTION)).actionGet().isExists() ); 
		
		client.admin().indices().delete(new DeleteIndexRequest(CATALOG+"b")).actionGet();
	}
	
	@Test
	public void dropCatalogTest() throws UnsupportedOperationException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {

		Row row = new Row();
		Map<String, Cell> cells = new HashMap<>();
		cells.put("name1", new Cell("value1"));
		cells.put("name2", new Cell(2));
		row.setCells(cells);

		((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
		
		refresh();

		assertEquals("Catalog deleted", true, client.admin().indices().exists(new IndicesExistsRequest(CATALOG)).actionGet().isExists()); 
		
		((IMetadataProvider) stratioElasticConnector.getMedatadaProvider()).dropCatalog(CATALOG);
		
		refresh();

		assertEquals("Catalog deleted", false, client.admin().indices().exists(new IndicesExistsRequest(CATALOG)).actionGet().isExists()); 

	}
	
	
}