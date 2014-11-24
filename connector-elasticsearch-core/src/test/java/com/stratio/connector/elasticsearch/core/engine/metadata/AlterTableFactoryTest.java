package com.stratio.connector.elasticsearch.core.engine.metadata; 

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.AlterOptions;
import com.stratio.crossdata.common.exceptions.UnsupportedException;

/** 
* AlterTableFactory Tester. 
* 
* @author <Authors name> 
* @since <pre>nov 24, 2014</pre> 
* @version 1.0 
*/ 
public class AlterTableFactoryTest {


@Before
public void before() throws Exception {


} 


/** 
* 
* Method: createHandeler(AlterOptions alterOptions) 
* 
*/ 
@Test
public void testCreateAddColumnHandeler() throws Exception {
    AlterOptions alterOptions = new AlterOptions(AlterOperation.ADD_COLUMN, Collections.EMPTY_MAP,null);
    assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(),
            AlterTableFactory.createHandeler(alterOptions).getClass().getCanonicalName());
}


    @Test(expected = UnsupportedException.class)

    public void testAlterColumnHandeler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ALTER_COLUMN, Collections.EMPTY_MAP,null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(),
                AlterTableFactory.createHandeler(alterOptions).getClass().getCanonicalName());
    }


    @Test(expected = UnsupportedException.class)

    public void testAlterOptionsHandeler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.ALTER_OPTIONS, Collections.EMPTY_MAP,null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(),
                AlterTableFactory.createHandeler(alterOptions).getClass().getCanonicalName());
    }


    @Test(expected = UnsupportedException.class)

    public void testDropColumnHandeler() throws Exception {
        AlterOptions alterOptions = new AlterOptions(AlterOperation.DROP_COLUMN, Collections.EMPTY_MAP,null);
        assertEquals("The instance is correct", AddColumnHandler.class.getCanonicalName(),
                AlterTableFactory.createHandeler(alterOptions).getClass().getCanonicalName());
    }

} 
