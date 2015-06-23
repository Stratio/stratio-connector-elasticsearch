package com.stratio.connector.elasticsearch.core.engine.utils;


import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SelectCreatorTest {

    @Test
    public void testModify(){
        SearchRequestBuilder requestBuilder = mock(SearchRequestBuilder.class);
        Select select = mock(Select.class);
        SelectCreator selectCreator = new SelectCreator();

        Map<Selector, String> columnMap = new HashMap<>();
        when(select.getColumnMap()).thenReturn(columnMap);

        ColumnSelector columnSelector = mock(ColumnSelector.class);
        ColumnName columnName = mock(ColumnName.class);
        when(columnSelector.getColumnName()).thenReturn(columnName);
        when(columnName.getName()).thenReturn("columnName");

        columnMap.put(columnSelector, "columnName");

        //Experimentation
        selectCreator.modify(requestBuilder, select);

        //Expectations
        String[] fields = new String[]{"columnName"};
        Mockito.verify(requestBuilder).addFields(fields);
    }
}