/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.elasticsearch.core.engine.utils;

import static org.jgroups.util.Util.assertNotNull;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Before;
import org.junit.Test;

import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.IntegerSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;

/** 
* QueryBuilderCreator Tester. 
* 
* @author <Authors name> 
* @since <pre>nov 21, 2014</pre> 
* @version 1.0 
*/ 
public class QueryBuilderCreatorTest {

    private static final String CATALOG_NAME = "catalog_name";
    private static final String TABLE_NAME = "table_name";
    private static final String COLUMN_NAME = "column_name";
    private static final String LT_QUERY = "{\"bool\":{\"must\":{\"range\":{\"column_name\":{\"from\":null,\"to\":\"5\",\"include_lower\":true,\"include_upper\":false}}}}}";
    private static final String LET_QUERY = "{\"bool\":{\"must\":{\"range\":{\"column_name\":{\"from\":null,\"to\":\"5\",\"include_lower\":true,\"include_upper\":true}}}}}";
    private static final String GT_QUERY = "{\"bool\":{\"must\":{\"range\":{\"column_name\":{\"from\":\"5\",\"to\":null,\"include_lower\":false,\"include_upper\":true}}}}}";
    private static final String GET_QUERY = "{\"bool\":{\"must\":{\"range\":{\"column_name\":{\"from\":\"5\",\"to\":null,\"include_lower\":true,\"include_upper\":true}}}}}";
    private QueryBuilderCreator queryBuilderCreator;
    private static String EQ_QUERY = "{\"bool\":{\"must\":{\"match\":{\"column_name\":{\"query\":\"5\"," +
            "\"type\":\"boolean\"}}}}}";


@Before
public void before() throws Exception {

    queryBuilderCreator = new QueryBuilderCreator();
} 


/** 
* 
* Method: createBuilder(Collection<Filter> filters) 
* 
*/ 
@Test
public void testCreateBuilderEQ() throws Exception {
    Collection<Filter> filter = new ArrayList<>();

    createFilter(filter, Operations.DELETE_PK_EQ, Operator.EQ);

    QueryBuilder queryBuilder= queryBuilderCreator.createBuilder(filter);

    assertNotNull("the Query builder is not null",queryBuilder);
    assertEquals("The queryIsCorrect",EQ_QUERY, queryBuilder.toString().replaceAll("\n", "").replaceAll(" ", ""));

}


    /**
     *
     * Method: createBuilder(Collection<Filter> filters)
     *
     */
    @Test
    public void testCreateBuilderLT() throws Exception {
        Collection<Filter> filter = new ArrayList<>();

        createFilter(filter, Operations.DELETE_PK_LT, Operator.LT);

        QueryBuilder queryBuilder= queryBuilderCreator.createBuilder(filter);

        assertNotNull("the Query builder is not null",queryBuilder);
        assertEquals("The queryIsCorrect",LT_QUERY, queryBuilder.toString().replaceAll("\n", "").replaceAll(" ", ""));

    }


    /**
     *
     * Method: createBuilder(Collection<Filter> filters)
     *
     */
    @Test
    public void testCreateBuilderLET() throws Exception {
        Collection<Filter> filter = new ArrayList<>();

        createFilter(filter, Operations.DELETE_PK_LET, Operator.LET);


        QueryBuilder queryBuilder= queryBuilderCreator.createBuilder(filter);

        assertNotNull("the Query builder is not null",queryBuilder);
        assertEquals("The queryIsCorrect",LET_QUERY, queryBuilder.toString().replaceAll("\n", "").replaceAll(" ", ""));

    }


    @Test
    public void testCreateBuilderGT() throws Exception {
        Collection<Filter> filter = new ArrayList<>();

        createFilter(filter, Operations.DELETE_PK_GT, Operator.GT);


        QueryBuilder queryBuilder= queryBuilderCreator.createBuilder(filter);

        assertNotNull("the Query builder is not null",queryBuilder);
        assertEquals("The queryIsCorrect",GT_QUERY, queryBuilder.toString().replaceAll("\n", "").replaceAll(" ", ""));

    }

    @Test
    public void testCreateBuilderGET() throws Exception {
        
        Collection<Filter> filter = new ArrayList<>();

        createFilter(filter, Operations.DELETE_PK_GET, Operator.GET);


        QueryBuilder queryBuilder= queryBuilderCreator.createBuilder(filter);

        assertNotNull("the Query builder is not null",queryBuilder);
        assertEquals("The queryIsCorrect",GET_QUERY, queryBuilder.toString().replaceAll("\n", "").replaceAll(" ", ""));

    }
    
    



    private void createFilter(Collection<Filter> filter, Operations operations, Operator operator) {
        filter.add(new Filter(operations,new Relation(new ColumnSelector(new ColumnName(CATALOG_NAME,
                TABLE_NAME,
                COLUMN_NAME)), operator,new IntegerSelector(5))));
    }

} 
