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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Test;

import com.stratio.connector.elasticsearch.core.engine.ElasticsearchQueryEngine;
import com.stratio.connector.elasticsearch.core.engine.ElasticsearchStorageEngine;
import com.stratio.connector.elasticsearch.ftest.ConnectionTest;
import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.exception.UnsupportedOperationException;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.logicalplan.Project;
import com.stratio.meta.common.result.QueryResult;


public class LimitTest extends ConnectionTest {

	public static final String COLUMN_TEXT = "text";
	public static final String COLUMN_AGE = "age";
	public static final String COLUMN_MONEY = "money";

	@Test
	public void limitTest() throws Exception {

		insertRow(1, "text", 10, 20);// row,text,money,age
		insertRow(2, "text", 9, 17);
		insertRow(3, "text", 11, 26);
		insertRow(4, "text", 10, 30);
		insertRow(5, "text", 20, 42);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = createLogicalPlan(2);

		// limit 2
		QueryResult queryResult = (QueryResult) ((ElasticsearchQueryEngine) stratioElasticConnector.getQueryEngine()).execute(CLUSTER_NODE_NAME,logicalPlan);

		assertEquals(2, queryResult.getResultSet().size());

	}
	
	private LogicalWorkflow createLogicalPlan(int limit) {

		List<LogicalStep> stepList = new ArrayList<>();

		List<ColumnName> columns = new ArrayList<>();

		columns.add(new ColumnName(CATALOG,COLLECTION, COLUMN_TEXT)); //REVIEW todo esto se ha cambiado para que compile
		columns.add(new ColumnName(CATALOG,COLLECTION, COLUMN_AGE));
        TableName tableName = new TableName(CATALOG,COLLECTION);
		Project project =  new Project(null,tableName, columns);
		stepList.add(project);

		stepList.add(new Limit(limit));

		return new LogicalWorkflow(stepList);

	}

	private void insertRow(int ikey, String texto, int money, int age) throws UnsupportedOperationException, ExecutionException, UnsupportedException {

		Row row = new Row();
	    Map<String, Cell> cells = new HashMap<>();
	    cells.put(COLUMN_TEXT, new Cell(texto+ikey));
	    cells.put(COLUMN_AGE, new Cell(age));
	    cells.put(COLUMN_MONEY, new Cell(money));
	    row.setCells(cells);        
	    ((ElasticsearchStorageEngine) stratioElasticConnector.getStorageEngine()).insert(CLUSTER_NODE_NAME,new TableMetadata(new TableName(CATALOG,COLLECTION),null,null,null,null,null), row);
	        
	    
	}

}