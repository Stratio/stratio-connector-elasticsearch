package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.statements.structures.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by lcisneros on 25/09/15.
 */
public class RowSorterTest {

    @Test
    public void testSimpleSort() {
        List<OrderByClause> orderByClauses = new ArrayList<>();
        ColumnSelector column = new ColumnSelector(new ColumnName(new TableName("catalog","table"), "a"));
        column.setAlias("a");

        orderByClauses.add(new OrderByClause(OrderDirection.ASC, column));

        RowSorter sorter = new RowSorter(orderByClauses);

        List<Row> rows = new ArrayList<>();
        Row a = new Row();
        a.addCell("a", new Cell(3));
        a.addCell("b", new Cell("c"));
        a.addCell("c", new Cell(13));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(2));
        a.addCell("b", new Cell("b"));
        a.addCell("c", new Cell(12));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(1));
        a.addCell("b", new Cell("a"));
        a.addCell("c", new Cell("11"));
        rows.add(a);


        Collections.sort(rows, sorter);

        assertEquals(1, rows.get(0).getCell("a").getValue());
        assertEquals(2, rows.get(1).getCell("a").getValue());
    }


    @Test
    public void testSimpleSortDesc() {
        List<OrderByClause> orderByClauses = new ArrayList<>();
        ColumnSelector column = new ColumnSelector(new ColumnName(new TableName("catalog","table"), "a"));
        column.setAlias("a");
        orderByClauses.add(new OrderByClause(OrderDirection.DESC, column));

        RowSorter sorter = new RowSorter(orderByClauses);

        List<Row> rows = new ArrayList<>();
        Row a = new Row();
        a.addCell("a", new Cell(3));
        a.addCell("b", new Cell("c"));
        a.addCell("c", new Cell(13));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(2));
        a.addCell("b", new Cell("b"));
        a.addCell("c", new Cell(12));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(1));
        a.addCell("b", new Cell("a"));
        a.addCell("c", new Cell("11"));
        rows.add(a);


        Collections.sort(rows, sorter);

        assertEquals(3, rows.get(0).getCell("a").getValue());
        assertEquals(2, rows.get(1).getCell("a").getValue());
    }

    @Test
    public void testDoubleSort() {
        List<OrderByClause> orderByClauses = new ArrayList<>();
        ColumnSelector column1 = new ColumnSelector(new ColumnName(new TableName("catalog","table"), "a"));
        column1.setAlias("a");
        ColumnSelector column2= new ColumnSelector(new ColumnName(new TableName("catalog","table"), "b"));
        column2.setAlias("b");
        orderByClauses.add(new OrderByClause(OrderDirection.ASC, column1));
        orderByClauses.add(new OrderByClause(OrderDirection.ASC, column2));

        RowSorter sorter = new RowSorter(orderByClauses);

        List<Row> rows = new ArrayList<>();
        Row a = new Row();
        a.addCell("a", new Cell(1));
        a.addCell("b", new Cell("c"));
        a.addCell("c", new Cell(13));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(2));
        a.addCell("b", new Cell("b"));
        a.addCell("c", new Cell(12));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(1));
        a.addCell("b", new Cell("a"));
        a.addCell("c", new Cell("11"));
        rows.add(a);


        Collections.sort(rows, sorter);

        assertEquals(1, rows.get(0).getCell("a").getValue());
        assertEquals("a", rows.get(0).getCell("b").getValue());
        assertEquals("c", rows.get(1).getCell("b").getValue());
    }



    @Test
    public void testTripleSort() {
        List<OrderByClause> orderByClauses = new ArrayList<>();
        ColumnSelector column1 = new ColumnSelector(new ColumnName(new TableName("catalog","table"), "a"));
        column1.setAlias("a");
        ColumnSelector column2= new ColumnSelector(new ColumnName(new TableName("catalog","table"), "b"));
        column2.setAlias("b");
        ColumnSelector column3= new ColumnSelector(new ColumnName(new TableName("catalog","table"), "c"));
        column3.setAlias("c");

        orderByClauses.add(new OrderByClause(OrderDirection.ASC, column1));
        orderByClauses.add(new OrderByClause(OrderDirection.ASC, column2));
        orderByClauses.add(new OrderByClause(OrderDirection.ASC, column3));

        RowSorter sorter = new RowSorter(orderByClauses);

        List<Row> rows = new ArrayList<>();
        Row a = new Row();
        a.addCell("a", new Cell(1));
        a.addCell("b", new Cell("a"));
        a.addCell("c", new Cell(13));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(2));
        a.addCell("b", new Cell("b"));
        a.addCell("c", new Cell(12));
        rows.add(a);

        a = new Row();
        a.addCell("a", new Cell(1));
        a.addCell("b", new Cell("a"));
        a.addCell("c", new Cell(11));
        rows.add(a);


        Collections.sort(rows, sorter);

        assertEquals(1, rows.get(0).getCell("a").getValue());
        assertEquals(1, rows.get(1).getCell("a").getValue());
        assertEquals(11, rows.get(0).getCell("c").getValue());
        assertEquals(13, rows.get(1).getCell("c").getValue());

        assertEquals("b", rows.get(2).getCell("b").getValue());
    }
}
