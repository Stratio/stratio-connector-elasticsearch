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
package com.stratio.connector.elasticsearch.core.configuration;

import java.util.HashSet;
import java.util.Set;

import com.stratio.connector.meta.ConnectionConfiguration;
import com.stratio.connector.meta.ConnectionOption;

/**
 * Created by jmgomez on 22/07/14.
 */
public class ConnectionConfigurationCreator {

    private static Set<ConnectionConfiguration> configuration = new HashSet<>();

    static {
        configuration.add(new ConnectionConfiguration(ConnectionOption.HOST_IP, false, true));
        //TODO false=>node, true =>tranpsort
        configuration.add(new ConnectionConfiguration(ConnectionOption.HOST_PORT, false, true));
    }

    /**
     * Return the connectionConfiguration options.
     *
     * @return the connectionConfiguration options.
     */
    public static Set<ConnectionConfiguration> getConfiguration() {
        return configuration;
    }
}