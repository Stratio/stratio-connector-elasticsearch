# Fist Steps #

Here is a group of examples to illustrate the the use of Elasticsearch Crossdata connector .

## Connector Configuration ##
First of all [Stratio Crossdata 0.1.1] https://github.com/Stratio/crossdata.git is needed and must be installed. The 
server and shell must be running.

In Crossdata Shell we need to add the Datastore Manifest.

```
   > add datastore "<path_to_manifest_folder>/ElasticSearchDataStore.xml";
```

The output must be:

```
   [INFO|Shell] Response time: 0 seconds    
   [INFO|Shell] OK
```

Now we need to add the ConnectorManifest.

```
   > add connector "<path_to_manifest_folder>/ElasticSearchConnector.xml";  
```
The output must be:


```
   [INFO|Shell] Response time: 0 seconds    
   [INFO|Shell] OK
```

At this point we have informed to crossdata the connector options and operations. Now we go to configure the 
datastore cluster.

```
> ATTACH CLUSTER elasticsearchCluster ON DATASTORE elasticsearch WITH OPTIONS {'Hosts': '[Ip1, Ip2,..,Ipn]', 
'Port': '[Port1,Port2,...,Portn]'};
```

The output must be:
```
  Result: QID: 82926b1e-2f72-463f-8164-98969c352d40
  Cluster attached successfully
```

Now we must run the connector

```
  > <path_to_connector_executable>/connector-elasticsearch-core-<version> start
```

To ensure that the connector is online we can execute the crossdata shell command

```
  > describe connectors;
```

And the output mus be

```
Connector: connector.elasticsearchconnector	ONLINE	[]	[datastore.elasticsearch]	akka.tcp://CrossdataServerCluster@127.0.0.1:46646/user/ConnectorActor/
```

The last step is to attach the connector to the cluster created before.

```
  >  ATTACH CONNECTOR elasticsearchconnector TO elasticsearchCluster  WITH OPTIONS {};
```

The output must be
```
CONNECTOR attached successfully
```

## Create catalog ##

Now we'll create the catalog and table which use later in the next steps.

To create catalog we must execute.

```
    > CREATE CATALOG highschool;
```
The output must be.

```
CREATE CATALOG highschool;
```

## Create table ##

To create the table we must execute the next command.

```
  > CREATE TABLE highschool.students ON CLUSTER elasticsearchCluster (id int PRIMARY KEY, name text, age int, 
enrolled boolean);
```

And the output must show.

```
TABLE created successfully
```


## Insert ##

At first we must to insert some rows in the table created before.
```
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (1, 'Jhon', 16,true);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (2, 'Eva',20,true);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (3, 'Lucie',18,true);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (4, 'Cole',16,true);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (5, 'Finn',17.false);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (6, 'Violet',21,false);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (7, 'Beatrice',18,true);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (8, 'Henry',16,false);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (9, 'Tom',17,true);
  >  INSERT INTO highschool.students(id, name,age,enrolled) VALUES (10, 'Betty',19,true);
```

For each row the output must be.

```
STORED successfully
```
## Select ###
Now we go to execute a series of queries and we show the expected result.

### Select All ###

```
 > SELECT * FROM highschool.students;
 Partial result: true
 ----------------------------------
 | age | id | enrolled | name     | 
 ----------------------------------
 | 16  | 4  | true     | Cole     | 
 | 17  | 9  | true     | Tom      | 
 | 16  | 8  | false    | Henry    | 
 | 18  | 3  | true     | Lucie    | 
 | 20  | 2  | true     | Eva      | 
 | 18  | 7  | true     | Beatrice | 
 | 16  | 1  | true     | Jhon     | 
 | 21  | 6  | false    | Violet   | 
 ----------------------------------
```

### Select by id and project ###
```
  > SELECT name, enrolled FROM highschool.students where id = 1;
  Partial result: true
  -------------------
  | name | enrolled | 
  -------------------
  | Jhon | true     | 
  -------------------
```

### Select with alias ###

```
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
```

### Limit the rows returned ### 
```
  >  SELECT * FROM highschool.students LIMIT 3;
  Partial result: true
  -------------------------------
  | age | id | enrolled | name  | 
  -------------------------------
  | 16  | 4  | true     | Cole  | 
  | 17  | 9  | true     | Tom   | 
  | 18  | 3  | true     | Lucie | 
  -------------------------------
```
## Delete ##
For this examples we'll execute many delete instruction and we'll show the table evolution. 


```
 ----------------------------------
 | age | id | enrolled | name     | 
 ----------------------------------
 | 16  | 4  | true     | Cole     | 
 | 17  | 9  | true     | Tom      | 
 | 16  | 8  | false    | Henry    | 
 | 18  | 3  | true     | Lucie    | 
 | 20  | 2  | true     | Eva      | 
 | 18  | 7  | true     | Beatrice | 
 | 16  | 1  | true     | Jhon     | 
 | 21  | 6  | false    | Violet   | 
 ----------------------------------
  >  DELETE FROM highschool.students  WHERE id = 1;
  ----------------------------------
  | age | id | enrolled | name     | 
  ----------------------------------
  | 16  | 4  | true     | Cole     | 
  | 17  | 9  | true     | Tom      | 
  | 16  | 8  | false    | Henry    | 
  | 18  | 3  | true     | Lucie    | 
  | 20  | 2  | true     | Eva      | 
  | 18  | 7  | true     | Beatrice | 
  | 21  | 6  | false    | Violet   | 
  ----------------------------------
  > DELETE FROM highschool.students  WHERE id < 3;
  ----------------------------------
  | age | id | enrolled | name     | 
  ----------------------------------
  | 16  | 4  | true     | Cole     | 
  | 17  | 9  | true     | Tom      | 
  | 16  | 8  | false    | Henry    | 
  | 18  | 3  | true     | Lucie    | 
  | 18  | 7  | true     | Beatrice | 
  | 21  | 6  | false    | Violet   | 
  ----------------------------------
  > DELETE FROM highschool.students  WHERE age <= 17;
  ----------------------------------
  | age | id | enrolled | name     | 
  ----------------------------------
  | 18  | 3  | true     | Lucie    | 
  | 18  | 7  | true     | Beatrice | 
  | 21  | 6  | false    | Violet   | 
  ----------------------------------
  >  DELETE FROM highschool.students  WHERE id > 6;
  --------------------------------
  | age | id | enrolled | name   | 
  --------------------------------
  | 18  | 3  | true     | Lucie  | 
  | 21  | 6  | false    | Violet | 
  --------------------------------
  > DELETE FROM highschool.students  WHERE id >= 3;
```
At this point the table must be empty. The sentence select * from highschool.students must be return.

```
OK
Result page: 0
```
## Alter table ##
Now we'll alter the table structure.
```
  > ALTER TABLE highschool.students ADD surname TEXT;
```

After the alter operation we can insert the surname in the table.
```
	> INSERT INTO highschool.students(id, name,age,enrolled,surname) VALUES (10, 'Betty',19,true, 'Smith');
```
And table must contains the correct row.
```
  > SELECT * FROM highschool.students;
-----------------------------------------
| surname | age | id | enrolled | name  | 
-----------------------------------------
| Smith   | 19  | 10 | true     | Betty | 
-----------------------------------------
```

## Truncate table ## 
Now we go to truncate the table. To do these we must execute the sentence.

```
  > TRUNCATE highschool.students;
```

The output must be.
```
STORED successfully
 > SELECT * FROM highschool.students;
OK
Result page: 0
```
## Drop table ## 
To drop the table we must execute
```
  >  DROP TABLE if exists highschool.students;
TABLE dropped successfully

```


