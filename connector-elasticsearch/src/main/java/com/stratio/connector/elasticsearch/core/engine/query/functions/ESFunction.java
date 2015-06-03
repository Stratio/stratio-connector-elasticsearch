package com.stratio.connector.elasticsearch.core.engine.query.functions;

import com.stratio.crossdata.common.statements.structures.Selector;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

/**
 * Created by lcisneros on 2/06/15.
 */
public abstract class ESFunction {

    private String name;

    private List<Selector> paramareters;

    public ESFunction(List<Selector> paramareters) {
        this.paramareters = paramareters;
    }

    public List<Selector> getParamareters() {
        return paramareters;
    }

    public void setParamareters(List<Selector> paramareters) {
        this.paramareters = paramareters;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


    public abstract QueryBuilder buildQuery();
}
