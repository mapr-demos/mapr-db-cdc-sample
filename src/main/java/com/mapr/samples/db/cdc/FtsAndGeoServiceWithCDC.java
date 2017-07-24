package com.mapr.samples.db.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.ojai.FieldPath;
import org.ojai.KeyValue;
import org.ojai.Value;
import org.ojai.store.cdc.ChangeDataRecord;
import org.ojai.store.cdc.ChangeDataRecordType;
import org.ojai.store.cdc.ChangeNode;
import org.ojai.store.cdc.ChangeOp;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class FtsAndGeoServiceWithCDC {

  private static String CHANGE_LOG = "/demo_changelog:demo_table";
  private static String FTS_TOPIC = "/demo_app_stream:fts_service";
  private static String GEOS_TOPIC = "/demo_app_stream:geo_service";
  private static ObjectMapper jsonMapper = new ObjectMapper();


  public static void main(String[] args) {

    System.out.println("==== Start Application ===");


    // Producer used to send message to a new topics
    Properties producerProperties = new Properties();
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.put("streams.buffer.max.time.ms", "300");
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);


    // Consumer configuration
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty("group.id", "cdc.consumer.demo_table.fts_geo");
    consumerProperties.setProperty("enable.auto.commit", "true");
    consumerProperties.setProperty("auto.offset.reset", "latest");
    consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.setProperty("value.deserializer", "com.mapr.db.cdc.ChangeDataRecordDeserializer");


    // Consumer used to consume MapR-DB CDC events
    KafkaConsumer<byte[], ChangeDataRecord> consumer = new KafkaConsumer<byte[], ChangeDataRecord>(consumerProperties);
    consumer.subscribe(Arrays.asList(CHANGE_LOG));

    while (true) {
      ConsumerRecords<byte[], ChangeDataRecord> changeRecords = consumer.poll(500);
      Iterator<ConsumerRecord<byte[], ChangeDataRecord>> iter = changeRecords.iterator();

      while (iter.hasNext()) {
        ConsumerRecord<byte[], ChangeDataRecord> crec = iter.next();
        // The ChangeDataRecord contains all the changes made to a document
        ChangeDataRecord changeDataRecord = crec.value();
        String documentId = changeDataRecord.getId().getString();

        if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_INSERT) {
          System.out.println("\n\t Document Inserted " + documentId);
          insertAndUpdateDocument(changeDataRecord, producer);
        } else if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_UPDATE) {
          System.out.println("\n\t Document Updated " + documentId);
          insertAndUpdateDocument(changeDataRecord, producer);
        } else if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_DELETE) {
          System.out.println("\n\t Document Deleted " + documentId);
          deleteDocument(changeDataRecord, producer);
        }


      }
    }

  }


  /**
   * This method is used to capture all changes of documents (insert and update)
   * This method will :
   * * check if the First and Last Names have been updated/created and then "process" it
   * * check if the Address exists or has been updated and then "process" it
   *
   * @param changeDataRecord to process
   * @param producer         to post the messages created during CDC processing
   */
  private static void insertAndUpdateDocument(ChangeDataRecord changeDataRecord, KafkaProducer<String, String> producer) {
    String docId = changeDataRecord.getId().getString();


    // Create JSON Message for the Full Text Service
    ObjectNode indexingMessage = jsonMapper.createObjectNode();
    boolean sendIndexingMessage = false; // only send the message if at least one of the field is present
    ObjectNode fieldToIndex = jsonMapper.createObjectNode();
    indexingMessage.put("_id", docId);
    indexingMessage.put("operation", changeDataRecord.getType().toString());


    // Create JSON Message for the Address Service
    ObjectNode addressMessage = jsonMapper.createObjectNode();
    boolean sendAddressMessage = false; // only send the message if address is part of the operation
    addressMessage.put("_id", docId);
    addressMessage.put("operation", changeDataRecord.getType().toString());


    // Use the ChangeNode Iterator to capture all the individual changes
    Iterator<KeyValue<FieldPath, ChangeNode>> cdrItr = changeDataRecord.iterator();
    while (cdrItr.hasNext()) {
      Map.Entry<FieldPath, ChangeNode> changeNodeEntry = cdrItr.next();
      String fieldPathAsString = changeNodeEntry.getKey().asPathString();
      ChangeNode changeNode = changeNodeEntry.getValue();

      // When "INSERTING" a documen the field path is empty (new document)
      // and all the changes are made in a single object represented as a Map
      if (fieldPathAsString == null || fieldPathAsString.equals("")) { // Insert

        // get the value from the ChangeNode
        // and check if the fields firstName, LastName or Address are part of the operation
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
          addressMessage.set("address", jsonMapper.convertValue((Map) documentInserted.get("address"), JsonNode.class));
          sendAddressMessage = true;
        }


      } else {

        // when doing an update the database event is masde of one ChangeNode by field
        if (fieldPathAsString.equalsIgnoreCase("firstName")) {
          fieldToIndex.put("firstName", changeNode.getString());
          sendIndexingMessage = true;
        } else if (fieldPathAsString.equalsIgnoreCase("lastName")) {
          fieldToIndex.put("lastName", changeNode.getString());
          sendIndexingMessage = true;
        } else if (fieldPathAsString.equalsIgnoreCase("address")) {
          addressMessage.set("address", jsonMapper.convertValue(changeNode.getMap(), JsonNode.class));
          sendAddressMessage = true;
        }

      }


    }

    if (sendIndexingMessage) {
      indexingMessage.set("fields_to_index", fieldToIndex);

      producer.send(new ProducerRecord<String, String>(
              FTS_TOPIC,
              indexingMessage.toString()));

      System.out.println("\t   Posting to FTS Service " + indexingMessage);
    }


    if (sendAddressMessage) {
      producer.send(new ProducerRecord<String, String>(
              GEOS_TOPIC,
              addressMessage.toString()));

      System.out.println("\t   Posting to Geo Service " + addressMessage);
    }


  }


  /**
   * Send a delete message to the FTS Service topic
   *
   * @param changeDataRecord to process
   * @param producer         to post the messages created during CDC processing
   */
  private static void deleteDocument(ChangeDataRecord changeDataRecord, KafkaProducer<String, String> producer) {
    ObjectNode indexingMessage = jsonMapper.createObjectNode();
    boolean sendIndexingMessage = false; // only send the message if at least one of the field is present
    indexingMessage.put("_id", changeDataRecord.getId().getString());
    indexingMessage.put("operation", changeDataRecord.getType().toString());

    producer.send(new ProducerRecord<String, String>(
            "/demo_app_stream:fts_service",
            indexingMessage.toString()));

    System.out.println("\t   Posting to FTS Service " + indexingMessage);

  }


}
