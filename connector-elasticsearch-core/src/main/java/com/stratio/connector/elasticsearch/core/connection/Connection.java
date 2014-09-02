package com.stratio.connector.elasticsearch.core.connection;

/**
 * Created by jmgomez on 29/08/14.
 */
public abstract class Connection<T> {


    protected boolean isConnect = false;

    public abstract void close();

    public boolean isConnect(){

        return isConnect;
    }

    public abstract T getClient();

}
