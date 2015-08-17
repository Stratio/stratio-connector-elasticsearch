Fist Steps
**********

Here is a group of examples to illustrate the use of Elasticsearch
Crossdata connector.

Connector Configuration
-----------------------

First of all [Stratio Crossdata 0.2.0]
https://github.com/Stratio/crossdata.git is needed and must be
installed. The server and the Shell must be running.

In the Crossdata Shell we need to configure the datastore cluster.

::

    > ATTACH CLUSTER elasticsearchCluster ON DATASTORE elasticsearch WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
    ''Native Ports': '[TCPpot1,TCPport2,..,TCPportn]', 'Restful Ports':'[RestfulPort1,RestfulPort2,..,Restfuln]',
    'Cluster Name':'ES Cluster name to connect'};

The output must be:

::

      Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
      Cluster attached successfully

Now we must run the connector.
In the parent directory:

::

      > ./connector-elasticsearch/target/stratio-connector-elasticsearch/bin/stratio-connector-elasticsearch;

To ensure that the connector is online we can execute the crossdata
shell command

::

      > describe connectors;

And the output must be:

::

    Connector: connector.elasticsearchconnector ONLINE  []  [datastore.elasticsearch]   akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/

The last step is to attach the connector to the cluster created before.

::

      >  ATTACH CONNECTOR elasticsearchconnector TO elasticsearchCluster  WITH OPTIONS {};

The output must be

::

    Connected with SessionId=Connected successfully

Create catalog
--------------

Now we will create the catalog and the table which we will use later in
the next steps.

To create the catalog we must execute.

::

        > CREATE CATALOG highschool;

The output must be:

::

    CATALOG created successfully

Create table
------------

To create the table we must execute the next command.

::

      > CREATE TABLE highschool.students ON CLUSTER elasticsearchCluster (id int PRIMARY KEY, name text, age int, 
    enrolled boolean);

And the output must show.

::

    TABLE created successfully

Insert
------

At first we must insert some rows in the table created before.

::

      >  
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (1, 'Jhon', 16,true);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (2, 'Eva',20,true);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (3, 'Lucie',18,true);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (4, 'Cole',16,true);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (5, 'Finn',17.false);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (6, 'Violet',21,false);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (7, 'Beatrice',18,true);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (8, 'Henry',16,false);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (9, 'Tom',17,true);
        INSERT INTO highschool.students(id, name,age,enrolled) VALUES (10, 'Betty',19,true);
     >

For each row the output must be:

::

    STORED successfully

Select
------

Now we execute a set of queries and we will show the expected results.

Select All
~~~~~~~~~~

::

     > SELECT * FROM highschool.students;
     
     Partial result: true
     ----------------------------------
     | id | name     | age | enrolled |
     ----------------------------------
     | 4  | Cole     | 16  | true     |
     | 9  | Tom      | 17  | true     |
     | 3  | Lucie    | 18  | true     |
     | 8  | Henry    | 16  | false    |
     | 10 | Betty    | 19  | true     |
     | 2  | Eva      | 20  | true     |
     | 7  | Beatrice | 18  | true     |
     | 6  | Violet   | 21  | false    |
     | 1  | Jhon     | 16  | true     |
     ----------------------------------

Select by id and project
~~~~~~~~~~~~~~~~~~~~~~~~

::

      > SELECT name, enrolled FROM highschool.students where id = 1;
      
      Partial result: true
      -------------------
      | name | enrolled | 
      -------------------
      | Jhon | true     | 
      -------------------

Select with alias
~~~~~~~~~~~~~~~~~

::

       >  SELECT name as the_name, enrolled  as is_enrolled FROM highschool.students;
       
       Partial result: true
       --------------------------
       | the_name | is_enrolled | 
       --------------------------
       | Cole     | true        | 
       | Tom      | true        | 
       | Lucie    | true        | 
       | Henry    | false       | 
       | Eva      | true        | 
       | Beatrice | true        | 
       | Jhon     | true        | 
       | Violet   | false       | 
       --------------------------

Limit the numbers of rows returned
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

      >  SELECT * FROM highschool.students LIMIT 3;
      
      Partial result: true
    -------------------------------
    | id | name  | age | enrolled |
    -------------------------------
    | 4  | Cole  | 16  | true     |
    | 9  | Tom   | 17  | true     |
    | 3  | Lucie | 18  | true     |
    -------------------------------

Delete
------

For these examples we will execute many delete instructions and we will
show the table evolution.

::

     ----------------------------------
     | id | name     | age | enrolled |
     ----------------------------------
     | 4  | Cole     | 16  | true     |
     | 9  | Tom      | 17  | true     |
     | 3  | Lucie    | 18  | true     |
     | 8  | Henry    | 16  | false    |
     | 10 | Betty    | 19  | true     |
     | 2  | Eva      | 20  | true     |
     | 7  | Beatrice | 18  | true     |
     | 6  | Violet   | 21  | false    |
     | 1  | Jhon     | 16  | true     |
     ----------------------------------
     
      >  DELETE FROM highschool.students  WHERE id = 1;
      
    ----------------------------------
    | id | name     | age | enrolled |
    ----------------------------------
    | 4  | Cole     | 16  | true     |
    | 9  | Tom      | 17  | true     |
    | 3  | Lucie    | 18  | true     |
    | 8  | Henry    | 16  | false    |
    | 10 | Betty    | 19  | true     |
    | 2  | Eva      | 20  | true     |
    | 7  | Beatrice | 18  | true     |
    | 6  | Violet   | 21  | false    |
    ----------------------------------

      
      > DELETE FROM highschool.students  WHERE id < 3;
      
    ----------------------------------
    | id | name     | age | enrolled |
    ----------------------------------
    | 4  | Cole     | 16  | true     |
    | 9  | Tom      | 17  | true     |
    | 3  | Lucie    | 18  | true     |
    | 8  | Henry    | 16  | false    |
    | 10 | Betty    | 19  | true     |
    | 7  | Beatrice | 18  | true     |
    | 6  | Violet   | 21  | false    |
    ----------------------------------
      
      > DELETE FROM highschool.students  WHERE age <= 17;
      
    ----------------------------------
    | id | name     | age | enrolled |
    ----------------------------------
    | 3  | Lucie    | 18  | true     |
    | 10 | Betty    | 19  | true     |
    | 7  | Beatrice | 18  | true     |
    | 6  | Violet   | 21  | false    |
    ----------------------------------


      >  DELETE FROM highschool.students  WHERE id > 6;

    --------------------------------
    | id | name   | age | enrolled |
    --------------------------------
    | 3  | Lucie  | 18  | true     |
    | 6  | Violet | 21  | false    |
    --------------------------------

      
      > DELETE FROM highschool.students  WHERE id >= 3;

At this point the table must be empty. The sentence select \* from
highschool.students must be returned.

::

    OK
    Result page: 0

Alter table
-----------

Now we will alter the table structure.

::

      > ALTER TABLE highschool.students ADD surname TEXT;

After the alter operation we can insert the surname field in the table.

::

        > INSERT INTO highschool.students(id, name,age,enrolled,surname) VALUES (10, 'Betty',19,true, 'Smith');

And table must contain the row correctly.

::

      > SELECT * FROM highschool.students;
      
    -----------------------------------------
    | id | name  | age | enrolled | surname |
    -----------------------------------------
    | 10 | Betty | 19  | true     | Smith   |
    -----------------------------------------

Truncate table
--------------

Now we truncate the table. To do this we must execute the sentence.

::

      > TRUNCATE highschool.students;

The output must be:

::

    STORED successfully
     > SELECT * FROM highschool.students;
    OK
    Result page: 0

Drop table
----------

To drop the table we must execute

::

      >  DROP TABLE if exists highschool.students;
    TABLE dropped successfully

