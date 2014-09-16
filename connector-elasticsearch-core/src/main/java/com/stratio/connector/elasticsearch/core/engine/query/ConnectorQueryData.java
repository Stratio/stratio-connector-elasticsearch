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

package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.connector.meta.Limit;
import com.stratio.connector.meta.Sort;
import com.stratio.meta.common.logicalplan.Filter;
import com.stratio.meta.common.logicalplan.Project;
import org.elasticsearch.action.search.SearchType;

import java.util.ArrayList;
import java.util.Collection;

/**
 * This class is a representation of a ElasticSearch query.
 * Created by jmgomez on 15/09/14.
 */
public class ConnectorQueryData {

    /**
     * The search type.
     */
    private SearchType searchType = null;
    /**
     * The projection.
     */
    private Project projection = null;
    /**
     * The sort.
     */
    private Collection<Sort> sortList = new ArrayList<>();
    /**
     * The limit.
     */
    private Limit limit = null;
    /**
     * The filters.
     */
    private Collection<Filter> filterList = new ArrayList<>();


    /**
     * Add a sort.
     *
     * @param sort a sort.
     */
    public void addSort(Sort sort) {

        sortList.add(sort);
    }

    /**
     * This method ask query if has sort list.
     *
     * @return true if the query has sort list. False in other case.
     */

    public boolean hasSortList() {
        return !sortList.isEmpty();
    }


    /**
     * Return the sort list.
     *
     * @return the sort list.
     */
    public Collection<Sort> getSortList() {
        return sortList;
    }

    /**
     * This method ask query if has limit.
     *
     * @return true if the query has limit. False in other case.
     */

    public boolean hasLimitStep() {

        return limit != null;
    }

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
     * get the limit.
     *
     * @return the limit.
     */
    public Limit getLimit() {

        return limit;
    }

    /**
     * Set the limit.
     *
     * @param limit the limit.
     */
    public void setLimit(Limit limit) {
        this.limit = limit;
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
     * Get the search type.
     *
     * @return the search type.
     */
    public SearchType getSearchType() {
        return searchType;
    }

    /**
     * set the search type.
     *
     * @param searchType the search type.
     */
    public void setSearchType(SearchType searchType) {

        this.searchType = searchType;
    }

}
