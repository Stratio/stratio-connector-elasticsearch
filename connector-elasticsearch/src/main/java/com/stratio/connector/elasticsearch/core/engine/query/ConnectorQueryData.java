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

import java.util.ArrayList;
import java.util.Collection;

import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;

/**
 * This class is a representation of a ElasticSearch query.
 * <p/>
 * <p/>
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryData {

    /**
     * The projection.
     */
    private Project projection = null;

    /**
     * The filters.
     */
    private Collection<Filter> filterList = new ArrayList<>();
    /**
     * The matchList.
     */
    private Collection<Filter> matchList = new ArrayList<>();

    /**
     * The select.
     */
    private Select select;
    /**
     * The limit.
     */
    private Limit limit;

    /**
     * Add a filter.
     *
     * @param filter the filter.
     */
    public void addFilter(Filter filter) {

        filterList.add(filter);
    }

    /**
     * This method ask query if has projection.
     *
     * @return true if the query has projection. False in other case.
     */
    public boolean hasProjection() {

        return projection != null;
    }

    /**
     * Get the projection.
     *
     * @return the projection,
     */
    public Project getProjection() {

        return projection;
    }

    /**
     * Set the projection.
     *
     * @param projection the projection.
     */
    public void setProjection(Project projection) {

        this.projection = projection;
    }

    /**
     * Get the filter.
     *
     * @return the filter.
     */
    public Collection<Filter> getFilter() {
        return filterList;
    }

    /**
     * Add filter to the matchList.
     *
     * @param filter the filter to add.
     */
    public void addMatch(Filter filter) {

        matchList.add(filter);
    }

    /**
     * Return The matchList.
     *
     * @return the matchList
     */
    public Collection<Filter> getMatchList() {
        return matchList;
    }

    /**
     * This method ask query if has filter list.
     *
     * @return true if the query has filter list. False in other case.
     */
    public boolean hasFilterList() {
        return !filterList.isEmpty();
    }

    /**
     * return the select.
     *
     * @return the select.
     */
    public Select getSelect() {
        return select;
    }

    /**
     * Add a select type.
     *
     * @param select the select.
     */
    public void setSelect(Select select) {
        this.select = select;

    }

    /**
     * Return the limit.
     *
     * @return the limit.
     */
    public Limit getLimit() {
        return limit;
    }

    /**
     * set the limit.
     *
     * @param limit the limit.
     */
    public void setLimit(Limit limit) {
        this.limit = limit;
    }
}
