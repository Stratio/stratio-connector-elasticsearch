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

/**
 * Created by jmgomez on 21/07/14.
 */
public class ConnectionConfiguration {

    private ConnectionOption connectionOption;
    private Boolean mandatory;
    private Boolean multivalued;

    public ConnectionConfiguration(ConnectionOption connectionOption, Boolean mandatory, Boolean multivalued) {
        this.connectionOption = connectionOption;
        this.mandatory = mandatory;
        this.multivalued = multivalued;
    }

    public Boolean isMandatory() {
        return mandatory;
    }

    public Boolean isMultivalued() {
        return multivalued;
    }

    public ConnectionOption getConnectionOption() {
        return connectionOption;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionConfiguration that = (ConnectionConfiguration) o;

        if (connectionOption != that.connectionOption) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return connectionOption != null ? connectionOption.hashCode() : 0;
    }

}