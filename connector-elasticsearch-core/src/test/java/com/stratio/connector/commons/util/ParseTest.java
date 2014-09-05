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

package com.stratio.connector.commons.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Parse Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>sep 3, 2014</pre>
 */
public class ParseTest {

    Parser parse;

    @Before
    public void before() throws Exception {
        parse = new Parser();
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void parseHosts() {

        String[] hosts = parse.hosts("10.70.90.12,10.70.90.14");

        assertEquals("The length is correct", 2, hosts.length);
        assertEquals("The fist ip is correct", "10.70.90.12", hosts[0]);
        assertEquals("The fist ip is correct", "10.70.90.14", hosts[1]);
    }


    @Test
    public void parsePorts() {

        String[] ips = parse.ports("2000,3000");

        assertEquals("The length is correct", 2, ips.length);
        assertEquals("The fist port is correct", "2000", ips[0]);
        assertEquals("The fist port is correct", "3000", ips[1]);
    }


}
