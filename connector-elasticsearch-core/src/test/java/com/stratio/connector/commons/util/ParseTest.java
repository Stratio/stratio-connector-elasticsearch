package com.stratio.connector.commons.util; 

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After;

import static org.junit.Assert.assertEquals;

/** 
* Parse Tester. 
* 
* @author <Authors name> 
* @since <pre>sep 3, 2014</pre> 
* @version 1.0 
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
    public void parseHosts(){

        String[] hosts = parse.hosts("10.70.90.12,10.70.90.14");

        assertEquals("The length is correct",2,hosts.length);
        assertEquals("The fist ip is correct","10.70.90.12",hosts[0]);
        assertEquals("The fist ip is correct","10.70.90.14",hosts[1]);
    }


    @Test
    public void parsePorts(){

        String[] ips = parse.ports("2000,3000");

        assertEquals("The length is correct",2,ips.length);
        assertEquals("The fist port is correct","2000",ips[0]);
        assertEquals("The fist port is correct","3000",ips[1]);
    }



} 
