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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.Sort;
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

public class OrderByTest extends ConnectionTest {


    public static final String COLUMN_TEXT = "text";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";
    
    public static final int SORT_AGE = 1;
    public static final int SORT_AGE_MONEY = 2;
    public static final int SORT_AGE_TEXT = 3;


    @Test
    public void sortFailTest() throws UnsupportedOperationException, ExecutionException {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,26);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         refresh();
         
         LogicalPlan logicalPlan = createLogicalPlan(SORT_AGE);
         
         //return COLUMN_TEXT order by age DESC

		try {
			((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
			fail();
		} catch (com.stratio.connector.meta.exception.UnsupportedOperationException e) {
			//ok
		} catch( Exception e){
			fail();
		}
         
     
        
    }

    @Test
    public void sortDescTest() throws  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException, UnsupportedException {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,26);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         refresh();
         
         LogicalPlan logicalPlan = createLogicalPlanLimit(SORT_AGE);
         
         //return COLUMN_TEXT order by age DESC

        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
			
    
	    assertEquals(5, queryResult.getResultSet().size());
	    
	    Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
	
	       
	    assertEquals("text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    
		}
    
    @Test
    public void sortTestMultifield() throws  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException, UnsupportedException {

    	 insertRow(1,"text",10,20);//row,text,money,age
         insertRow(2,"text",9,17);
         insertRow(3,"text",11,26);
         insertRow(4,"text",10,30);
         insertRow(5,"text",20,42);	
         insertRow(6,"text",10,10);	
         refresh();
         
         LogicalPlan logicalPlan = createLogicalPlanMultifield();
         
         //return COLUMN_TEXT order by money asc, age asc

        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(logicalPlan);
			
    
	    assertEquals(6, queryResult.getResultSet().size());
	    
	    Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
	
	       
	    assertEquals("text2", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text6", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text1", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text4", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text3", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    assertEquals("text5", rowIterator.next().getCell(COLUMN_TEXT).getValue());
	    
		}
    
    
    
    

	private void fail() {
		assertTrue(false);
		
	}




      	 
      	 
	private LogicalPlan createLogicalPlanLimit(int sortAge) {
	   	 
	   	 
	   	 List<LogicalStep> stepList = new ArrayList<>();

	     List<ColumnMetadata> columns = new ArrayList<>();
	     
	     Limit limit = new Limit(10);
	     stepList.add(limit);

	     columns.add(new ColumnMetadata(COLLECTION,COLUMN_TEXT));
	     columns.add(new ColumnMetadata(COLLECTION,COLUMN_AGE));
	     Project project = new Project(CATALOG, COLLECTION,columns);
	     stepList.add(project);
	     
	     
	        switch (sortAge){
	        	case SORT_AGE: stepList.add(new Sort(COLUMN_AGE, Sort.DESC)); break;
	        	
	// 2 Sort? o uno con lista de parámetros y luego lista de tipo ASC o DESC
//    	        	case SORT_AGE_MONEY: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
//    	        	case SORT_AGE_TEXT: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
	        }
	        return new LogicalPlan(stepList);

		}

   	
    
	private LogicalPlan createLogicalPlan(int sortAge) {
   	 
   	 
   	 List<LogicalStep> stepList = new ArrayList<>();

     List<ColumnMetadata> columns = new ArrayList<>();
     
     

     columns.add(new ColumnMetadata(COLLECTION,COLUMN_TEXT));
     columns.add(new ColumnMetadata(COLLECTION,COLUMN_AGE));
     Project project = new Project(CATALOG, COLLECTION,columns);
     stepList.add(project);
     
     
        switch (sortAge){
        	case SORT_AGE: stepList.add(new Sort(COLUMN_AGE, Sort.DESC)); break;
        	
// 2 Sort? o uno con lista de parámetros y luego lista de tipo ASC o DESC
//        	case SORT_AGE_MONEY: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
//        	case SORT_AGE_TEXT: stepList.add(createNotEqualsFilter(filterType, object)); stepList.add(createBetweenFilter(9,11)); break;
        }
        return new LogicalPlan(stepList);

	}

	private LogicalPlan createLogicalPlanMultifield() {
		 
	   	 List<LogicalStep> stepList = new ArrayList<>();

	     List<ColumnMetadata> columns = new ArrayList<>();
	     

	     columns.add(new ColumnMetadata(COLLECTION,COLUMN_TEXT));
	     columns.add(new ColumnMetadata(COLLECTION,COLUMN_AGE));
	     // money not required
	     Project project = new Project(CATALOG, COLLECTION,columns);
	     
	     stepList.add(project);
	     stepList.add(new Sort(COLUMN_MONEY, Sort.ASC));
	     stepList.add(new Sort(COLUMN_AGE, Sort.ASC));

	     Limit limit = new Limit(10);
	     stepList.add(limit);
	     
	     return new LogicalPlan(stepList);

	}


private void insertRow(int ikey, String texto, int money, int age) throws UnsupportedOperationException, ExecutionException{
     	
	Row row = new Row();
    Map<String, Cell> cells = new HashMap<>();
    cells.put(COLUMN_TEXT, new Cell(texto+ikey));
    cells.put(COLUMN_AGE, new Cell(age));
    cells.put(COLUMN_MONEY, new Cell(money));
    row.setCells(cells);        
    ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CATALOG, COLLECTION, row);
        
    }

 
}