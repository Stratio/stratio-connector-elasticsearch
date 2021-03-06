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

import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.Operator;

/**
 * The responsibility of this class is parser he LogicalWorkFlow to a QueryData. Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryParser {

    /**
     * this method transfor a logical workflow to a queryData.
     *
     * @param logicalWorkFlow
     *            the logical workflow.
     * @return the queryData.
     * @throws UnsupportedException
     *             if the logicalworkflow contains unsupported operations.
     * @throws ExecutionException
     *             if the execution fails.
     */
    public ConnectorQueryData transformLogicalWorkFlow(Project logicalWorkFlow) throws UnsupportedException,
                    ExecutionException {

        ConnectorQueryData queryData = new ConnectorQueryData();
        LogicalStep lStep = logicalWorkFlow;

        do {
            if (lStep instanceof Project) {
                if (!queryData.hasProjection()) {
                    queryData.setProjection((Project) lStep);
                } else {
                    throw new ExecutionException(" # Project > 1");
                }
            } else if (lStep instanceof Filter) {

                Filter step = (Filter) lStep;
                if (Operator.MATCH == step.getRelation().getOperator()) {
                    queryData.addMatch((Filter) lStep);
                } else {
                    queryData.addFilter((Filter) lStep);
                }
            } else if (lStep instanceof Select) {
                queryData.setSelect((Select) lStep);

            } else if (lStep instanceof Limit) {
                queryData.setLimit((Limit) lStep);
            } else {
                throw new UnsupportedException("LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported");
            }

            lStep = lStep.getNextStep();

        } while (lStep != null);

        checkSupportedQuery(queryData);

        return queryData;
    }

    /**
     * Check if the queryData is supported.
     *
     * @param queryData
     *            the qiery data.
     * @throws ExecutionException
     *             if there is no select.
     */
    private void checkSupportedQuery(ConnectorQueryData queryData) throws ExecutionException {
        if (queryData.getSelect() == null) {
            throw new ExecutionException("no select found");
        }

    }

}
