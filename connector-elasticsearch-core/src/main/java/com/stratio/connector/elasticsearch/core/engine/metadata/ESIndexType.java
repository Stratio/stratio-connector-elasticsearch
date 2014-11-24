package com.stratio.connector.elasticsearch.core.engine.metadata;

/**
 * Created by jmgomez on 21/11/14.
 */
public enum ESIndexType {
    NO("no"), ANALYZED("analyzed"),NOT_ANALYZED("not_analyzed");

    /**
     * Return Index code.
     * @return the index code.
     */
    public String getCode() {
        return code;
    }

    private final String code;

    ESIndexType(String code) {
        this.code = code;
    }


    public static ESIndexType getDefault(){
        return ANALYZED;
    }



}
