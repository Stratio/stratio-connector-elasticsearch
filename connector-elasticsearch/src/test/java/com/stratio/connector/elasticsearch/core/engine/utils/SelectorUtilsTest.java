package com.stratio.connector.elasticsearch.core.engine.utils;


import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SelectorUtilsTest {

    @Test
    public void testGetNullSelectorFieldName(){
        assertNull(SelectorUtils.getSelectorFieldName(null));
    }

    @Test
    public void testGetSelectorFieldName(){
        Selector selector = mock(Selector.class);
        ColumnName columnName = mock(ColumnName.class);
        when(selector.getColumnName()).thenReturn(columnName);
        when(columnName.getName()).thenReturn("fieldName");

        assertEquals("fieldName", SelectorUtils.getSelectorFieldName(selector));
    }

    @Test
    public void testGetSelectorUnknownFunctionName(){
        FunctionSelector functionSelector = mock(FunctionSelector.class);
        List<Selector> functionColumns = new ArrayList<Selector>();
        Selector selector = mock(Selector.class);
        ColumnName columnName = mock(ColumnName.class);
        Selector analyzer = mock(Selector.class);
        functionColumns.add(selector);
        functionColumns.add(analyzer);

        when(functionSelector.getFunctionName()).thenReturn("unknown");
        when(functionSelector.getFunctionColumns()).thenReturn(functionColumns);
        when(selector.getColumnName()).thenReturn(columnName);
        when(columnName.getName()).thenReturn("fieldName");
        when(analyzer.getStringValue()).thenReturn("analyzer");

        assertEquals("unknown", SelectorUtils.getSelectorFieldName(functionSelector));
    }

    @Test
    public void testGetSelectorFunctionSubFieldName(){
        FunctionSelector functionSelector = mock(FunctionSelector.class);
        List<Selector> functionColumns = new ArrayList<Selector>();
        Selector selector = mock(Selector.class);
        ColumnName columnName = mock(ColumnName.class);
        Selector analyzer = mock(Selector.class);
        functionColumns.add(selector);
        functionColumns.add(analyzer);

        when(functionSelector.getFunctionName()).thenReturn("sub_field");
        when(functionSelector.getFunctionColumns()).thenReturn(functionColumns);
        when(selector.getColumnName()).thenReturn(columnName);
        when(columnName.getName()).thenReturn("fieldName");
        when(analyzer.getStringValue()).thenReturn("analyzer");

        assertEquals("fieldName.analyzer", SelectorUtils.getSelectorFieldName(functionSelector));
    }
}