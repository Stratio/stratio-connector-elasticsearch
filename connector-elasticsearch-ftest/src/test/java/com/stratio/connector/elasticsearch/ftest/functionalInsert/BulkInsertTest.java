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

package com.stratio.connector.elasticsearch.ftest.functionalInsert;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.data.TableName;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.ValidationException;

/**
 * 
 */
public class BulkInsertTest extends ConnectionTest {

    final String COLLECTION = getClass().getSimpleName();

    @Test
    public void testBulkInsert() throws ExecutionException, ValidationException, UnsupportedOperationException, UnsupportedException {


        Set<Row> rows = new HashSet<Row>();
        
        for (int i = 0; i < 10; i++) {

            Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put("key", new Cell(i));
            cells.put("name1", new Cell("value1_R" + i));
            cells.put("name2", new Cell("value2_R" + i));
            cells.put("name3", new Cell("value3_R" + i));
            row.setCells(cells);
            rows.add(row);
        }


        TableMetadata targetTable = new TableMetadata(new TableName(CATALOG,COLLECTION),null,null,null,null,null);
        ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME, targetTable, rows);


        refresh(CATALOG);

        SearchResponse response = nodeClient.prepareSearch(CATALOG)
        		.setSearchType(SearchType.QUERY_THEN_FETCH) //si size es menor que el número de resultados podría no devolver todos los valores
        		.setTypes(COLLECTION)
        		.execute()
        		.actionGet();
        
        SearchHits hits = response.getHits();
   
        
        
        for(SearchHit hit: hits.hits()){
          assertEquals("The value is correct", "value1_R" + hit.getSource().get("key"), hit.getSource().get("name1"));
          assertEquals("The value is correct", "value2_R" + hit.getSource().get("key"), hit.getSource().get("name2"));
          assertEquals("The value is correct", "value3_R" + hit.getSource().get("key"), hit.getSource().get("name3"));

        }
        
        assertEquals("The records number is correct", 10, hits.totalHits());
        


    }
}
