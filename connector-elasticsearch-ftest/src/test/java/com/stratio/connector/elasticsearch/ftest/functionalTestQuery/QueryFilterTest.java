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

import com.stratio.meta.common.logicalplan.*;
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
import com.stratio.meta.common.result.QueryResult;


/**
 * Created by jmgomez on 17/07/14.
 */
public class QueryFilterTest extends ConnectionTest{

    public static final String COLUMN_1 = "bin1";
    public static final String COLUMN_2 = "bin2";
    public static final String COLUMN_3 = "bin3";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_MONEY = "money";

    private static final int EQUAL_FILTER =1;
    private static final int BETWEEN_FILTER =2;
    private static final int HIGH_FILTER =3;
    private static final int LOW_FILTER =4;
    private static final int HIGH_BETWEEN_FILTER =5;

    @Test
    public void selectFilterEqual() throws UnsupportedException, ExecutionException, com.stratio.connector.meta.exception.UnsupportedOperationException {


        insertRow(1,10,1);
        insertRow(2,9,1);
        insertRow(3,11,1);
        insertRow(4,10,1);
        insertRow(5,20,1);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = createLogicalPlan(EQUAL_FILTER);
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);
       
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }
        

        assertEquals("The record number is correct",6,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("money1"));



    }




    @Test
    public void selectFilterBetween() throws UnsupportedException, com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {



    	insertRow(1,1,10);
    	insertRow(2,1,9);
    	insertRow(3,1,11);
    	insertRow(4,1,10);
    	insertRow(5,1,20);
    	insertRow(6,1,11);
    	insertRow(7,1,8);
    	insertRow(8,1,12);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = createLogicalPlan(BETWEEN_FILTER);
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",14,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("money10"));
        assertTrue("Return correct record",proveSet.contains("money9"));
        assertTrue("Return correct record",proveSet.contains("money11"));
        assertTrue("Return correct record",proveSet.contains("age1"));

    }


   
    @Test
    public void selectHighBetween() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {



    	insertRow(1,10, 15);
    	insertRow(2,9,10);
    	insertRow(3,11,9);
    	insertRow(4,10,7);
    	insertRow(5,7,9);
    	insertRow(6,11,100);
    	insertRow(7,8,1);
    	insertRow(8,12,10);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = createLogicalPlan(HIGH_BETWEEN_FILTER);
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",8,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r8"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("age11"));
        assertTrue("Return correct record",proveSet.contains("money9"));
        assertTrue("Return correct record",proveSet.contains("money10"));
        assertTrue("Return correct record",proveSet.contains("age12"));

    }



    @Test
    public void selectFilterHigh() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {



    	insertRow(1,10,1);
    	insertRow(2,9,1);
    	insertRow(3,11,1);
    	insertRow(4,10,1);
    	insertRow(5,20,1);
    	insertRow(6,7,1);
    	insertRow(7,8,1);
    	insertRow(8,12,1);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(HIGH_FILTER);
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct",15,proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r5"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r5"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r8"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("age11"));
        assertTrue("Return correct record",proveSet.contains("age12"));
        assertTrue("Return correct record",proveSet.contains("age20"));
        assertTrue("Return correct record",proveSet.contains("money1"));




    }


    @Test
    public void selectFilterLow() throws UnsupportedException,  com.stratio.connector.meta.exception.UnsupportedOperationException, ExecutionException {



    	insertRow(1,10,1);
    	insertRow(2,9,1);
    	insertRow(3,11,1);
    	insertRow(4,10,1);
    	insertRow(5,20,1);
    	insertRow(6,7,1);
    	insertRow(7,8,1);
    	insertRow(8,12,1);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(LOW_FILTER);
        QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);
        
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while(rowIterator.hasNext()){
        	Row row = rowIterator.next();
            for (String cell:row.getCells().keySet()){
            	proveSet.add(cell+row.getCell(cell).getValue());
            }
        }

        assertEquals("The record number is correct", 15, proveSet.size());
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record",proveSet.contains("bin1ValueBin1_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r7"));
        assertTrue("Return correct record",proveSet.contains("bin2ValueBin2_r7"));
        assertTrue("Return correct record",proveSet.contains("age10"));
        assertTrue("Return correct record",proveSet.contains("age9"));
        assertTrue("Return correct record",proveSet.contains("age7"));
        assertTrue("Return correct record",proveSet.contains("age8"));
        assertTrue("Return correct record",proveSet.contains("money1"));

    }


    private LogicalWorkflow createLogicalPlan(int filterType) {
        List<LogicalStep> stepList = new ArrayList<>();
        List<ColumnName> columns = new ArrayList<>();

        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_1));
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_2));
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_AGE));
        columns.add(new ColumnName(CATALOG,COLLECTION,COLUMN_MONEY));
        TableName tableName = new TableName(CATALOG,COLLECTION); //REVIEW modificado para que compile
        Project project = new Project(null,tableName,columns);
        stepList.add(project);
        if (EQUAL_FILTER==filterType || HIGH_FILTER == filterType || LOW_FILTER == filterType || HIGH_BETWEEN_FILTER == filterType) stepList.add(createEqualsFilter(filterType));
        if (BETWEEN_FILTER==filterType || HIGH_BETWEEN_FILTER==filterType) stepList.add(createBetweenFilter());
        return new LogicalWorkflow(stepList);
    }

    private LogicalStep  createBetweenFilter() {
//        Relation relation = new RelationBetween(COLUMN_MONEY);
//        relation.setType(Relation.TYPE_BETWEEN);
//        List<Term<?>> terms = new ArrayList<>();
//        terms.add(new IntegerTerm("9"));
//        terms.add(new IntegerTerm("11"));
//        relation.setTerms(terms);
//        Filter f = new Filter(Operations.SELECT_WHERE_BETWEEN, RelationType.BETWEEN, relation);
//        return f;
        return null; //REVIEW para que compile por la nueva version de meta
    }

    private Filter createEqualsFilter(int filterType) {
//        Relation relation = new RelationCompare(COLUMN_AGE);
//        relation.setType(Relation.TYPE_COMPARE);
//        if (filterType==EQUAL_FILTER)
//            relation.setOperator("=");
//        if (filterType==HIGH_FILTER || filterType == HIGH_BETWEEN_FILTER)
//            relation.setOperator(">=");
//        if (filterType==LOW_FILTER)
//            relation.setOperator("<=");
//        List<Term<?>> terms = new ArrayList<>();
//        IntegerTerm term = new IntegerTerm("10");
//        terms.add(term);
//        relation.setTerms(terms);
//        return new Filter( Operations.SELECT_WHERE_MATCH , RelationType.COMPARE, relation);
        return null; //REVIEW para que compile por la nueva version de meta
    }

    
        private void insertRow(int ikey, int age, int money) throws UnsupportedOperationException, ExecutionException, UnsupportedException {
 	
        	Row row = new Row();
            Map<String, Cell> cells = new HashMap<>();
            cells.put(COLUMN_1, new Cell("ValueBin1_r"+ikey));
            cells.put(COLUMN_2, new Cell("ValueBin2_r"+ikey));
            cells.put(COLUMN_3, new Cell("ValueBin3_r"+ikey));
            cells.put(COLUMN_AGE, new Cell(age));
            cells.put(COLUMN_MONEY, new Cell(money));
            row.setCells(cells);        
            ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME,new TableMetadata(new TableName(CATALOG, COLLECTION),null,null,null,null,null), row);
            
        }


}
