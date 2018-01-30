# MapR-DB Change Data Capture Sample Code

This project shows the basic of MapR-DB Change Data Capture (CDC) API allowing application to capture database events.

## Introduction

The Change Data Capture (CDC) system allows you to capture changes made to data records in MapR-DB tables (JSON or binary) 
and propagate them to a MapR-ES topic. These data changes are the result of inserts, updates, and deletions and are called change data records. 
Once the change data records are propagated to a topic, a consumer application is used to read and process them.

The steps to build the CDC application are:

1. Create a MapR-DB Table
1. Create a stream that will receive all the database events
1. Configure the table to send event to the streams you have created: Adding a Change Data log
1. Create a consumer application to process database events


In this example you will use a MapR-DB JSON table named `/demo_table_json` that contains user information such as *first name*, *last name*, *age*, *email* and *address*. The Application (`FtsAndGeoServiceWithCDC`) analyze the events coming from the changelog to post new messages when the first and/or last names or when the address have been created or modified, or when a document is deleted. The sample application flow is:

![Application Flow](https://github.com/mapr-demos/mapr-db-cdc-sample/blob/master/doc/sample-app-flow.png "Application Flow")


The messages posted by the application could then be consumed by new services to:

* index names into a full text index to support advanced queries like soundex, fuzzy search, ...
* get the latitude and longitude from the address and save it back into the MapR-DB document.


**Prerequisites**

* MapR Converged Data Platform 6.0 
* JDK 8
* Maven 3.x



## Developing the application


### 1- Create a MapR-DB Table

This project uses the same table that the one used in the [MapR-DB JSON and OJAI 2.0 example](https://github.com/mapr-demos/ojai-2-examples)

If the table does not exist already, open a terminal window on your MapR 6.0 Cluster, and run the following command to create a JSON Table:

```
$ maprcli table create -path /demo_table_json -tabletype json
```

This command has created a new MapR-DB JSON Table, note that it is also possible to use MapR-DB CDC on binary tables, but this example is based on JSON table.


### 2- Creating the MapR Streams and Topics


**Changelog Stream**

In a terminal window on your MapR 6.0 cluster run the following command to create a stream:

```
$  maprcli stream create -path /demo_changelog -ischangelog true -consumeperm p
```

This command:

* Create a new streams at the following path `/demo_changelog`
* `ischangelog` set to true to configure the stream to store change log
* `-consumeperm p` set the changelog consumer presentation to "public" allowing any application to subscribe to the events.

**Application Stream**

In this application the database events are process by a consumer that publish new message on application topics that can be consumes by new services. Create the application topics:

```

$ maprcli stream create -path /demo_app_stream -produceperm p -consumeperm p -topicperm p

$ maprcli stream topic create  -path /demo_app_stream -topic fts_service -partitions 3

$ maprcli stream topic create  -path /demo_app_stream -topic geo_service -partitions 3


```



### 3- Adding the Change Log to the table

You have created a JSON table, and changelog stream, let's now use the `maprcli` command to capture the events and send them to the streams.
 In a terminal  enter the following command:
 
 ```
 $ maprcli table changelog add -path /demo_table_json -changelog /demo_changelog:demo_table_json 
 ```
 
 This command associate the `demo_table_json` table events with the `/demo_changelog:demo_table_json` topic.
 
 
 
###4- Build and Run the MapR-DB CDC Application
 


Build the application using Apache Maven:

```
$ mvn clean package
```

Copy the file to your cluster:

```
$ scp ./target/maprdb-cdc-sample-1.0-SNAPSHOT.jar mapr@mapr60:/home/mapr/ 
```

where mapr60 is one of the nodes of your cluster.


Run the CDC Application

```
$ java -cp maprdb-cdc-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath` com.mapr.samples.db.cdc.json.FtsAndGeoServiceJSONWithCDC
```

Open a new terminal and run the following commands, to create, update and delete documents

```
$ mapr dbshell

maprdb mapr:> insert /demo_table_json --value '{"_id":"user0010", "firstName" : "Matt", "lastName" : "Porker" , "age" : 34 }'

maprdb mapr:> update /demo_table_json --id user0010 --m '{ "$set":[  { "address":{"city":"San Jose","state":"CA","street":"320 Blossom Hill Road","zipCode":9519} }] }'

maprdb mapr:> update /demo_table_json --id user0010 --m '{ "$set":[ {"lastName":"Parker"},  { "address":{"city":"San Jose","state":"CA","street":"330 Blossom Hill Road","zipCode":9519} }] }'

maprdb mapr:> delete /demo_table_json --id user0010 

```

Each operation made in the MapR DB Shell generates some entries in the changelog. 

The first operation insert a new document, and the application capture the first and last name and post the value on the `/demo_app_stream:fts_service` topic. You can see in the terminal the following message 

```
Document Inserted "user0010"
  Posting to FTS Service {"_id":"user0010","operation":"RECORD_INSERT", "type":"json","fields_to_index":{"firstName":"Matt","lastName":"Porker"}}
``` 

The next operation add the address to the user profile, doing an update do the document; the CDC application capture the address and send it to the `/demo_app_stream:geo_service`.

The the operation updates two attributes, the last name and the address; the CDC application capture these changes and post messages and the two topics.

Finally the last operation deletes the document; the CDC application send a delete message with the document id on `/demo_app_stream:fts_service`.


## Developing a MapR DB Change Data Capture Application

This project is an example allowing you to understand how to use MapR DB CDC feature; the following section explain the main steps to build your own project.

### Maven Dependencies

To use MapR-DB CDC you must add the MapR Maven Repository and the MapR OJAI Dependencies to your project

MapR Maven Repository

```xml
    <repository>
      <id>mapr-releases</id>
      <url>http://repository.mapr.com/maven/</url>
    </repository>
```


MapR-DB CDC Dependency

```xml
    <dependency>
      <groupId>com.mapr.db</groupId>
      <artifactId>maprdb-cdc</artifactId>
      <version>6.0.0-mapr</version>
    </dependency>
```

### Using the MapR CDC API

A MapR CDC application is built the same way than any MapR-ES application. In this example we use a Java application with a `main()` method.

**1- Create The Consumer**

The first thing to do is to configure the consumer using Java properties. This could be externalized in a file, for simplicity reason the consumer properties are hard coded in the application code.

```java
    // Consumer configuration
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("group.id", "cdc.consumer.demo_table.json.fts_geo");
    consumerProperties.setProperty("enable.auto.commit", "true");
    consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.setProperty("value.deserializer", "com.mapr.db.cdc.ChangeDataRecordDeserializer");
```

The properties are the same as any MapR-ES or Apache Kafka application:

* `group.id` that identies the group of consumer
* `key.deserializer` an array of bytes created by the CDC gateway
* `value.deserializer` the value deserializer, MapR CDC uses a optimized serialization format for all the events, so you must specify the `com.mapr.db.cdc.ChangeDataRecordDeserializer` deserializer.



Create the consumer and subscribe to the Changelog, that is a MapR-ES topic.

```java
    // Consumer used to consume MapR-DB CDC events
    KafkaConsumer<byte[], ChangeDataRecord> consumer = new KafkaConsumer<byte[], ChangeDataRecord>(consumerProperties);
    consumer.subscribe(Arrays.asList("/demo_changelog:demo_table_json"));

```

The consumer is created using a key (bytes[]) and a `ChangeDataRecord` object for the value.


**2- Consume the events**

You can now listen to the event and process each `ChangeDataRecord`.

```java
    while (true) {
      ConsumerRecords<byte[], ChangeDataRecord> changeRecords = consumer.poll(500);
      Iterator<ConsumerRecord<byte[], ChangeDataRecord>> iter = changeRecords.iterator();

      while (iter.hasNext()) {
        ConsumerRecord<byte[], ChangeDataRecord> crec = iter.next();
        // The ChangeDataRecord contains all the changes made to a document
        ChangeDataRecord changeDataRecord = crec.value();
        String documentId = changeDataRecord.getId().getString();
        // process events
        ...
        ...
      }
    }

```

In the `iter.hasNext()` loop, start by extracting:

* the `ChangeDataRecord` using `crec.value()` method
* the document id using `changeDataRecord.getId().getString()`


**3- Process Change Data Records**

It is now time to process the Change Data Records, based on the type of event (insert, update, delete), using the `changeDataRecord.getType()` method. You can use the `ChangeDataRecordType` class to check the type.

**Processing Deletes**

Look at the `deleteDocument()` method in the sample application.

Processing a `delete` is a simple operation since the operation is based a single change data record, so you can directly get the document id using `changeDataRecord.getId()` and then process the document deletion.


**Processing Inserts and Updates**

Look at the `insertAndUpdateDocument()` method in the sample application.

Document mutations are stored into a list of `ChangeNodes`, that you retrieve using the following code:

```java
    // Use the ChangeNode Iterator to capture all the individual changes
    Iterator<KeyValue<FieldPath, ChangeNode>> cdrItr = changeDataRecord.iterator();
    while (cdrItr.hasNext()) {
      Map.Entry<FieldPath, ChangeNode> changeNodeEntry = cdrItr.next();
      String fieldPathAsString = changeNodeEntry.getKey().asPathString();
      ChangeNode changeNode = changeNodeEntry.getValue();
      ...
      ...
    }
```

* get the iterator of ChangeNode using `changeDataRecord.iterator()` and loop on them
* retrieve the change node entry and for each of them to extract:
  * the updated field path using `changeNodeEntry.getKey().asPathString()`
  * the `ChangeNode` using  `changeNodeEntry.getValue()` that contains the change information.

It is quite common when you want with CDC to capture the change of a subset of fields, for example in this case we just need to 
loop at `firstName`, `lastName` and `address`. To check this it depends of the type of change node operation.

***Inserting a document***

When **inserting a new document**, you have a single ChangeNode object in the iterator, the field path is empty, and the value contains the full document as a Map. 
To access the field names and values you can use the following logic:

```java
     if (fieldPathAsString == null || fieldPathAsString.equals("")) { // Insert
        Map<String, Object> documentInserted = changeNode.getMap();
        if (documentInserted.containsKey("firstName")) {
          fieldToIndex.put("firstName", (String) documentInserted.get("firstName"));
          sendIndexingMessage = true;
        }
        if (documentInserted.containsKey("lastName")) {
          fieldToIndex.put("lastName", (String) documentInserted.get("lastName"));
          sendIndexingMessage = true;
        }
        if (documentInserted.containsKey("address")) {
          addressMessage.set("address", jsonMapper.convertValue((Map)documentInserted.get("address"), JsonNode.class) );
          sendAddressMessage = true;
        }     
     }
```

To process the document insert the code:

* checks of the field path is null or empty
* gets the created document `using changeNode.getMap()`
* processes each field using the Java Map API.
* gets the first and last name Strings using `(String) documentInserted.get("firstName")`
* gets the address, that is a subdocument stored as a Map, using `(Map)documentInserted.get("address")`

This application creates new JSON documents (`fieldToIndex`, `addressMessage`) to send them to another topic.

***Updating a document***

When *updating a document*, the iterator contains one ChangeNode by updated field. You can then access the field path and valye directly as follow:

```java
        if (fieldPathAsString.equalsIgnoreCase("firstName")) {
          fieldToIndex.put("firstName", changeNode.getString());
          sendIndexingMessage = true;
        } else if (fieldPathAsString.equalsIgnoreCase("lastName")) {
          fieldToIndex.put("lastName", changeNode.getString());
          sendIndexingMessage = true;
        } else if (fieldPathAsString.equalsIgnoreCase("address")) {
          addressMessage.set("address", jsonMapper.convertValue( changeNode.getMap(), JsonNode.class) );
          sendAddressMessage = true;
        }
```

To process the document update the code:

* checks the field path using `if (fieldPathAsString.equalsIgnoreCase("firstName"))`
* gets the value using the proper method, depending of the expected type of the value:
  * `changeNode.getString()` for the first and last name
  * `changeNode.getMap()` for the address


## Create a CDC application for MapR-DB Binary table


We will use the same approach for the binary table:

* a new topic in the changelog for the binary table events : `/demo_changelog:demo_table_binary`
* a new CDC consumer that will listen to the changes and post changes to the FTS topic : `demo_app_stream:fts_service`

For this example we will limit the events to the `default:firstName` and `default:lastName` columns.


### 1- Create a MapR-DB Table and column family

```
$ maprcli table create -path /demo_table_binary -tabletype binary


$ maprcli table cf create -path /demo_table_binary -cfname default

   ```

### 2- Adding the Change Log to the table

You have created a JSON table, and changelog stream, let's now use the `maprcli` command to capture the events and send them to the streams.
 In a terminal  enter the following command:
 
 ```
 $ maprcli table changelog add -path /demo_table_binary -changelog /demo_changelog:demo_table_binary 
 
 ```
 
 This command associate the `demo_table_binary` table events with the `/demo_changelog:demo_table_binary` topic.
 

### 3- Build and Run the MapR-DB CDC Application

Build the application using Apache Maven:

```
$ mvn clean package
```

Copy the file to your cluster:

```
$ scp ./target/maprdb-cdc-sample-1.0-SNAPSHOT.jar mapr@mapr60:/home/mapr/ 
```

where mapr60 is one of the nodes of your cluster.


Run the CDC Application

```
$ java -cp maprdb-cdc-sample-1.0-SNAPSHOT.jar:`mapr clientclasspath`:`hbase classpath` com.mapr.samples.db.cdc.binary.FtsAndGeoServiceBinaryWithCDC
```

Open a new terminal and run the following commands, to create, update and delete rows

```
$ hbase shell

hbase(main):001:0> put '/demo_table_binary' , 'user010' , 'default:firstName', 'John' 

hbase(main):002:0> put '/demo_table_binary' , 'user010' , 'default:lastName', 'Doe' 

hbase(main):003:0> deleteall '/demo_table_binary' , 'user010'

```

The CDC application should print the following information:

```
	 Document Updated user010
	   Posting to FTS Service {"_id":"user010","operation":"RECORD_UPDATE","type":"binary","fields_to_index":{"firstName":"John"}}

	 Document Updated user010
	   Posting to FTS Service {"_id":"user010","operation":"RECORD_UPDATE","type":"binary","fields_to_index":{"lastName":"Doe"}}

	 Document Deleted user010
	   Posting to FTS Service {"_id":"user010","operation":"RECORD_DELETE"}
```


### 4- Application Code

The code used to process binary table events is very similar to the JSON table one, and available in the `FtsAndGeoServiceBinaryWithCDC.java` class.

### MapR-DB Binary Maven Dependency

Add the following dependency to your project:

```xml
    <dependency>
      <groupId>org.apache.hbase</groupId>
      <artifactId>hbase-client</artifactId>
      <version>1.1.8-mapr-1710</version>
    </dependency>
```

This will be used to deserialized the table content that is stored as Bytes.

#### 1. Create a MapR Streams Consumer

```java

    // Consumer configuration
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("group.id", "cdc.consumer.demo_table.binary.fts");
    consumerProperties.setProperty("enable.auto.commit", "true");
    consumerProperties.setProperty("auto.offset.reset", "latest");
    consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.setProperty("value.deserializer", "com.mapr.db.cdc.ChangeDataRecordDeserializer");


    // Consumer used to consume MapR-DB CDC events
    KafkaConsumer<byte[], ChangeDataRecord> consumer = new KafkaConsumer<byte[], ChangeDataRecord>(consumerProperties);
    consumer.subscribe(Arrays.asList("/demo_changelog:demo_table_binary"));

```

The properties are the same as any MapR-ES or Apache Kafka application:

group.id that identies the group of consumer
key.deserializer an array of bytes created by the CDC gateway
value.deserializer the value deserializer, MapR CDC uses a optimized serialization format for all the events, so you must specify the com.mapr.db.cdc.ChangeDataRecordDeserializer deserializer.
Create the consumer and subscribe to the Changelog, that is a MapR-ES topic.

```java
    // Consumer used to consume MapR-DB CDC events
    KafkaConsumer<byte[], ChangeDataRecord> consumer = new KafkaConsumer<byte[], ChangeDataRecord>(consumerProperties);
    consumer.subscribe(Arrays.asList("/demo_changelog:demo_table_binary"));
```

#### 2- Consume the events

You can now listen to the event and process each ChangeDataRecord.

```java

    while (true) {
      ConsumerRecords<byte[], ChangeDataRecord> changeRecords = consumer.poll(500);
      Iterator<ConsumerRecord<byte[], ChangeDataRecord>> iter = changeRecords.iterator();

      while (iter.hasNext()) {
        ConsumerRecord<byte[], ChangeDataRecord> crec = iter.next();
        // The ChangeDataRecord contains all the changes made to a document
        ChangeDataRecord changeDataRecord = crec.value();

        // get document ID from the binary key
        String documentId = Bytes.toString(changeDataRecord.getId().getBinary().array());
        ...
        ...

      }
    }
```

In the iter.hasNext() loop, start by extracting:

* the ChangeDataRecord using `crec.value()` method
* the rowkey using `changeDataRecord.getId()`, to get the value as String you must convert it using the following code `Bytes.toString(changeDataRecord.getId().getBinary().array())`


#### 3- Process Change Data Records

It is now time to process the Change Data Records, based on the type of event (insert, update, delete), using the `changeDataRecord.getType()` method. You can use the `ChangeDataRecordType` class to check the type.

**Processing Deletes**

Look at the `deleteDocument()` method in the sample application.

Processing a delete is a simple operation since the operation is based a single change data record, so you can directly get the document id using `changeDataRecord.getId()` and then process the document deletion.

**Processing Inserts and Updates**

Look at the `insertAndUpdateDocument()` method in the sample application.

Document mutations are stored into a list of ChangeNodes, that you retrieve using the following code:

```java
    // Use the ChangeNode Iterator to capture all the individual changes
    Iterator<KeyValue<FieldPath, ChangeNode>> cdrItr = changeDataRecord.iterator();
    while (cdrItr.hasNext()) {
      Map.Entry<FieldPath, ChangeNode> changeNodeEntry = cdrItr.next();
      String fieldPathAsString = changeNodeEntry.getKey().asPathString();
      ChangeNode changeNode = changeNodeEntry.getValue();

      // when doing an update the database event is masde of one ChangeNode by field
      if (fieldPathAsString.equalsIgnoreCase("default.firstName")) { // name of the field including column family
        // extract the value as a string since we know that default.firstName is a string
        fieldToIndex.put("firstName", Bytes.toString(changeNode.getBinary().array()));
        sendIndexingMessage = true;
      } else if (fieldPathAsString.equalsIgnoreCase("default.lastName")) {
        fieldToIndex.put("lastName", Bytes.toString(changeNode.getBinary().array()));
        sendIndexingMessage = true;
      }
    }
```


* get the iterator of ChangeNode using changeDataRecord.iterator() and loop on them
retrieve the change node entry and for each of them to extract:
* the updated field path using `changeNodeEntry.getKey().asPathString()`, in the contect of binary the valye is `column_family:column`, for example `default.firstName`.
* the ChangeNode using `changeNodeEntry.getValue()` that contains the change information.
* the values is a set of bytes in a binary table, so you need to convert it into a type that could be used by your target application, for example a String with ` Bytes.toString(changeNode.getBinary().array())`


## Conclusion

In this application you have learned:

* how to configure the Change Data Capture for a MapR DB Table
* write an application that capture events and use them.

