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


import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
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
 */
public class SimpleInsertTest extends ConnectionTest {


    @Test
    public void testSimpleInsert() throws ExecutionException, ValidationException, UnsupportedOperationException, UnsupportedException {


        testSimpleInsertConnection(CLUSTER_TRANSPORT_NAME,transportClient);
        testSimpleInsertConnection(CLUSTER_NODE_NAME,nodeClient);
        

    }

    private void testSimpleInsertConnection(ClusterName cluesterName, Client nativeClient ) throws UnsupportedException, ExecutionException {
        System.out.println("*********************************** INIT FUNCTIONAL TEST testSimpleInsert "+cluesterName.getName()+" ***********************************");

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put("name1", new Cell("value1"));
        cells.put("name2", new Cell(2));
        cells.put("name3", new Cell(true));
        row.setCells(cells);

        ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(cluesterName,  new TableMetadata(new TableName(CATALOG, COLLECTION),null,null,null,null,null), row);

        refresh(CATALOG);

        SearchResponse response = nativeClient.prepareSearch(CATALOG)
        		.setSearchType(SearchType.QUERY_THEN_FETCH) //si size es menor que el número de resultados podría no devolver todos los valores
        		.setTypes(COLLECTION)
        		.execute()
        		.actionGet();

        SearchHits hits = response.getHits();


        for(SearchHit hit: hits.hits()){
        	 assertEquals("The value is correct "+cluesterName.getName(), "value1", hit.getSource().get("name1"));
        	 assertEquals("The value is correct "+cluesterName.getName(), 2, hit.getSource().get("name2"));
        	 assertEquals("The value is correct "+cluesterName.getName(), true, hit.getSource().get("name3"));
        }

        assertEquals("The records number is correct "+cluesterName.getName(), 1, hits.totalHits());
    }


}
