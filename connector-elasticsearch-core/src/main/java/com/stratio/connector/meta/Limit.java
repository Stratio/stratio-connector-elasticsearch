/*
 * Stratio Meta
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.meta;

import java.util.List;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.logicalplan.LogicalStep;

public class Limit extends LogicalStep {

    /**
     * Number of elements returned
     */
    private int number;

    public Limit(Operations operations) {
        super(operations);
    }
    //	public Limit(int num){
    //number = num;
    //}

    public int getLimit() {
        return number;
    }

    @Override
    public List<LogicalStep> getPreviousSteps() {
        return null;
    }

    @Override
    public LogicalStep getFirstPrevious() {
        return null;
    }
}
