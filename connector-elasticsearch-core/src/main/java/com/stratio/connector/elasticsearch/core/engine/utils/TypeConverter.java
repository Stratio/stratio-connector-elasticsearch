package com.stratio.connector.elasticsearch.core.engine.utils;

import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * Created by jmgomez on 24/11/14.
 */
public class TypeConverter {

    /**
     * Constructor.
     */
    private TypeConverter(){}


    private static final String ES_LONG = "long";
    /**
     * The elasticsearch boolean name.
     */
    private static final String ES_BOOLEAN = "boolean";
    /**
     * The elasticsearch double name.
     */
    private static final String ES_DOUBLE = "double";
    /**
     * The elasticsearch float name.
     */
    private static final String ES_FLOAT = "float";
    /**
     * The elasticsearch integer name.
     */
    private static final String ES_INTEGER = "integer";
    /**
     * The elasticsearch string name.
     */
    private static final String ES_STRING = "string";


    /**
     * This method translates the crossdata columnType to ElasticSearch type.
     *
     * @param columnType the crossdata column type.
     * @return the ElasticSearch columnType.
     * @throws com.stratio.crossdata.common.exceptions.UnsupportedException if the type is not supported.
     */
    public static String convert(ColumnType columnType) throws UnsupportedException {

        String type = "";
        switch (columnType) {
        case BIGINT:
            type = ES_LONG;
            break;
        case BOOLEAN:
            type = ES_BOOLEAN;
            break;
        case DOUBLE:
            type = ES_DOUBLE;
            break;
        case FLOAT:
            type = ES_FLOAT;
            break;
        case INT:
            type = ES_INTEGER;
            break;
        case TEXT:
        case VARCHAR:
            type = ES_STRING;
            break;
        default:
            throw new UnsupportedException("The type [" + columnType + "] is not supported in ElasticSearch");
        }
        return type;
    }
}
