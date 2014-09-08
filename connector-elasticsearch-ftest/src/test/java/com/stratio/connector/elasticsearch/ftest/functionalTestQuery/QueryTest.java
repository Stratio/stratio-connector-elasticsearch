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
package com.stratio.connector.elasticsearch.ftest.functionalTestQuery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalPlan;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.metadata.structures.ColumnMetadata;
import com.stratio.meta.common.result.QueryResult;



/**
 * Created by jmgomez on 17/07/14.
 */
public class QueryTest extends ConnectionTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";

    @Test
    public void selectAllFromTable() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


        insertRow(1);
        insertRow(2);
        insertRow(3);
        insertRow(4);

        refresh();

        LogicalPlan logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",12,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r4"));

    }

    @Test
    public void selectWithoutRecords() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


        insertRow(1);

        refresh();

        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        columns.add(new ColumnMetadata(COLLECTION,COLUMN_1));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_2));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_3));
        Project project = new Project(CATALOG, "OTHER_COLL",columns);
        stepList.add(project);
        
        LogicalPlan logicalPlan = new LogicalPlan(stepList);
        
        
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

    }
    
    @Test
    public void selectFromTable() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


        insertRow(1);
        insertRow(2);
        insertRow(3);
        insertRow(4);
        
        insertRow(1,"type2");
        insertRow(2,"type2");

        refresh();

        LogicalPlan logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",12,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin3ValueBin3_r4"));

    }

    
    @Test
    public void selectAllFromTableCursor() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {

    	//insert more than SIZE elements
    	
        insertRow(1);
        insertRow(2);
        insertRow(3);
        insertRow(4);
        insertRow(5);
        insertRow(6);
        insertRow(7);
        insertRow(8);
        insertRow(9);
        insertRow(10);
        insertRow(11);
        insertRow(12);

        refresh();

        LogicalPlan logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
                proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",12*3,proveSet.size());

    }
    
    
    private LogicalPlan createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnMetadata> columns = new ArrayList<>();

        columns.add(new ColumnMetadata(COLLECTION,COLUMN_1));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_2));
        columns.add(new ColumnMetadata(COLLECTION,COLUMN_3));
        Project project = new Project(CATALOG, COLLECTION,columns);
        stepList.add(project);
        return new LogicalPlan(stepList);
    }

    private void insertRow(int ikey) throws UnsupportedOperationException, ExecutionException{
     	insertRow(ikey, COLLECTION);  
    }
    

private void insertRow(int ikey, String type) throws UnsupportedOperationException, ExecutionException{
     	
    	Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r"+ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r"+ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r"+ikey));
        row.setCells(cells);        
        ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG, type, row);
        
    }

}
