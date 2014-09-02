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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.stratio.meta.common.logicalplan.LogicalWorkflow;
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
 * 
 */
public class QueryProjectTest extends ConnectionTest {

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";

    @Test
    public void selectFilterProject() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {


        insertRow(1);
        insertRow(2);
        insertRow(3);
        insertRow(4);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan();
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine)stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);
        
        Set<Object> probeSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
                probeSet.add(cell+row.getCell(cell).getValue());
            }
        }
        

        assertEquals("The record number is correct",8,probeSet.size());
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r1"));
        assertFalse("This record should not be returned",probeSet.contains("bin3ValueBin3_r1"));
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r2"));
        assertFalse("This record should not be returned",probeSet.contains("bin3ValueBin3_r2"));
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r3"));
        assertFalse("This record should not be returned",probeSet.contains("bin3ValueBin3_r3"));
        assertTrue("Return correct record",probeSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",probeSet.contains("bin2ValueBin2_r4"));
        assertFalse("This record should not be returned",probeSet.contains("bin3ValueBin3_r4"));

    }






    private LogicalWorkflow createLogicalPlan() {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_1));
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_2));
        TableName tableName = new TableName(CATALOG,COLLECTION); //REVIEW modificado para que compile
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        return new LogicalWorkflow(stepList);
    }

private void insertRow(int ikey) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
     	
    	Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("ValueBin1_r"+ikey));
        cells.put(COLUMN_2, new Cell("ValueBin2_r"+ikey));
        cells.put(COLUMN_3, new Cell("ValueBin3_r"+ikey));
        row.setCells(cells);        
        ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME,new TableMetadata(new TableName(CATALOG, COLLECTION),null,null,null,null,null), row);
        
    }


}
