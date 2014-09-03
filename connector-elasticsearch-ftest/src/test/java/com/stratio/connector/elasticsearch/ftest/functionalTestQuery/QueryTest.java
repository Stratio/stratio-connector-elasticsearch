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

import com.stratio.meta.common.exceptions.ValidationException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;



/**
 * Created by jmgomez on 17/07/14.
 */
public class QueryTest extends ConnectionTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";

    @Test
    public void testselectAllFromTableTransport() throws ExecutionException, ValidationException, UnsupportedOperationException, UnsupportedException {
        selectAllFromTable(CLUSTER_TRANSPORT_NAME);
    }
    @Test
    public void testselectAllFromTabletNode() throws ExecutionException, ValidationException, UnsupportedOperationException, UnsupportedException {
        selectAllFromTable(CLUSTER_NODE_NAME);
    }


    @Test
    public void selectFromTableTransport() throws UnsupportedException, ExecutionException, UnsupportedOperationException {
        selectFromTable(CLUSTER_TRANSPORT_NAME);
    }


    @Test
    public void selectFromTableTransportNode() throws UnsupportedException, ExecutionException, UnsupportedOperationException {
        selectFromTable(CLUSTER_NODE_NAME);
    }

    @Test
    public void selectAllFromTableCursorTransport() throws UnsupportedException, ExecutionException, UnsupportedOperationException {
        selectAllFromTableCursor(CLUSTER_TRANSPORT_NAME);
    }

    @Test
    public void selectAllFromTableCursorNode() throws UnsupportedException, ExecutionException, UnsupportedOperationException {
        selectAllFromTableCursor(CLUSTER_NODE_NAME);
    }

    public void selectAllFromTable(ClusterName clusterNodeName) throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


        insertRow(1, clusterNodeName);
        insertRow(2, clusterNodeName);
        insertRow(3, clusterNodeName);
        insertRow(4, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(clusterNodeName,logicalPlan);
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

    

    public void selectFromTable(ClusterName clusterNodeName) throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


        insertRow(1, clusterNodeName);
        insertRow(2, clusterNodeName);
        insertRow(3, clusterNodeName);
        insertRow(4, clusterNodeName);

        insertRow(1,"type2", clusterNodeName);
        insertRow(2,"type2", clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(clusterNodeName,logicalPlan);
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

    

    public void selectAllFromTableCursor(ClusterName clusterNodeName) throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {

    	//insert more than SIZE elements
    	
        insertRow(1, clusterNodeName);
        insertRow(2, clusterNodeName);
        insertRow(3, clusterNodeName);
        insertRow(4, clusterNodeName);
        insertRow(5, clusterNodeName);
        insertRow(6, clusterNodeName);
        insertRow(7, clusterNodeName);
        insertRow(8, clusterNodeName);
        insertRow(9, clusterNodeName);
        insertRow(10, clusterNodeName);
        insertRow(11, clusterNodeName);
        insertRow(12, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(clusterNodeName,logicalPlan);
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
    
    
    private LogicalWorkflow createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_1));
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_2));
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_3));
        TableName tableName = new TableName(CATALOG,COLLECTION); //REVIEW para que compile
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }

    private void insertRow(int ikey, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
     	insertRow(ikey, COLLECTION, clusterNodeName);
    }
    

private void insertRow(int ikey, String type, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
     	
    	Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r"+ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r"+ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r"+ikey));
        row.setCells(cells);        
        ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(clusterNodeName,new TableMetadata(new TableName(CATALOG, type),null,null,null,null,null), row);
        
    }

}
