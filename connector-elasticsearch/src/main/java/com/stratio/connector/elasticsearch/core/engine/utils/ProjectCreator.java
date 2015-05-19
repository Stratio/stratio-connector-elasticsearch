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

import org.elasticsearch.action.search.SearchRequestBuilder;

import com.stratio.crossdata.common.logicalplan.Project;

/**
 * The responsibility of this class is create a Project creator.
 *
 * @author darroyo
 */
public class ProjectCreator {

    /**
     * This method modify the query to add a project.
     *
     * @param requestBuilder the request builder.
     * @param projection     the project.
     */
    public void modify(SearchRequestBuilder requestBuilder, Project projection) {

        requestBuilder.setIndices(projection.getCatalogName()).setTypes(projection.getTableName().getName());

    }

}