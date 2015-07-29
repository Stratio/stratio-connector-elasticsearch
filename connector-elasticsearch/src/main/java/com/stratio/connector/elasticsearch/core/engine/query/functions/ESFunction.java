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

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.statements.structures.FunctionRelation;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulate an Elastic Search "Function", The child of this abstract class will instance the
 * specific QueryBuilder that represents the filter required.
 */
public abstract class ESFunction {

    public static final String CONTAINS = "contains";
    public static final String MATCH_PHRASE = "match_phrase";
    public static final String MULTI_MATCH = "multi_match";
    public static final String MATCH_PREFIX = "match_prefix";
    public static final String MULTI_MATCH_FUZZY = "multi_match_fuzzy";
    public static final String FUZZY = "fuzzy";


    /**
     * Function Name
     */
    private String name;

    /**
     * The function parameters.
     */
    private List<Selector> parameters;

    /**
     * Default constructor.
     *
     * @param name Function Name
     * @param paramareters function parameters.
     */
    protected ESFunction(String name, List<Selector> paramareters) {
        this.parameters = paramareters;
        this.name = name;
    }

    public List<Selector> getParameters() {
        return parameters;
    }

    public String getName() {
        return name;
    }

    /**
     * Build a {@link org.elasticsearch.index.query.QueryBuilder} that satisfy this function.
     *
     * @return
     */
    public abstract QueryBuilder buildQuery() throws ExecutionException;


    /**
     * Builds a ESFunction using the FunctionRelation specification.cd
     * @param function
     * @return A ESFunction child.
     * @throws UnsupportedException is the FunctionRelation isn't supported.
     */
    public static ESFunction build(FunctionRelation function) throws UnsupportedException {
        List parameters = new ArrayList();
        parameters.addAll(function.getFunctionSelectors());

        switch(function.getFunctionName()){
            case CONTAINS:
                return new Match(parameters);
            case MATCH_PHRASE:
                return new MatchPhrase(parameters);
            case MULTI_MATCH:
                return new MultiMatch(parameters);
            case MATCH_PREFIX:
                return new MatchPrefix(parameters);
            case MULTI_MATCH_FUZZY:
                return new MultiMatchFuzzy(parameters);
            case FUZZY:
                return new Fuzzy(parameters);
            default:
                throw new UnsupportedException("The function [" + function.getFunctionName() + "] is not supported");
        }
    }

    /**
     * Checks function signature
     *
     * @param functionName                   function name
     * @param checkEmpty                     flag that indicates if empty values are allowed
     * @param expectedParameters      expected number of parameters
     * @param expectedSignature         expected function signature
     * @throws ExecutionException
     */
    public void validateFunctionSignature (String functionName, boolean checkEmpty, int expectedParameters, String expectedSignature)
            throws ExecutionException {

        if (null == parameters || parameters.size() != expectedParameters){
            throw new ExecutionException("Wrong number of parameters for \"" + functionName + "\" function. " +
                    "Expected signature: " + expectedSignature + ".");
        }
        if (checkEmpty){
            for (Selector parameter : parameters) {
                if (null == parameter || parameter.getStringValue().trim().isEmpty()){
                    throw new ExecutionException("Every parameter in \"" + functionName + "\" function must have a non empty value.");
                }
            }
        }
    }
}
