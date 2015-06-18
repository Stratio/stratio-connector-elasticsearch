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

package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.FunctionRelation;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;


public class ESFunctionTest {

    @Test(expected = UnsupportedException.class)
    public void testFailBuildFunction() throws UnsupportedException {
        buildFunction("not supported", null);
    }

    private void buildFunction(String functionName, Class expectedClass) throws UnsupportedException {

        FunctionRelation function = Mockito.mock(FunctionRelation.class);
        Mockito.when(function.getFunctionName()).thenReturn(functionName);
        Mockito.when(function.getFunctionSelectors()).thenReturn(Collections.<Selector>emptyList());

        //Experimentation
        ESFunction result = ESFunction.build(function);

        //Expectations
        Assert.assertEquals(result.getClass(), expectedClass);
    }


}
