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

package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.connector.commons.engine.query.ProjectValidator;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/**
 * Created by jmgomez on 17/12/14.
 */
public class ESProjectParsedValidator implements ProjectValidator {

    /**
     * This method validate the project parsed to ElasticSearch.
     * 
     * @param projectParsed
     *            the project parsed.
     * @throws UnsupportedException
     *             if a logicalStep is not supported
     *
     */
    @Override
    public void validate(ProjectParsed projectParsed) throws UnsupportedException {
        if (projectParsed.getWindow() != null) {
            throw new UnsupportedException("ElasticSearch don't support Window Operation");
        }

    }
}
