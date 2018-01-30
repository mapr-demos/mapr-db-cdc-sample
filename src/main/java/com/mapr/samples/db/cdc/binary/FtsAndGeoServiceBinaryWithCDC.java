package com.mapr.samples.db.cdc.binary;

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
import org.ojai.store.cdc.ChangeDataRecord;
import org.ojai.store.cdc.ChangeDataRecordType;
import org.ojai.store.cdc.ChangeNode;

import org.apache.hadoop.hbase.util.Bytes;


import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

@SuppressWarnings("Duplicates")
public class FtsAndGeoServiceBinaryWithCDC {

  private static String CHANGE_LOG = "/demo_changelog:demo_table_binary";
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
    consumerProperties.setProperty("group.id", "cdc.consumer.demo_table.binary.fts");
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

        // get document ID from the binary key
        String documentId = Bytes.toString(changeDataRecord.getId().getBinary().array());
        if (changeDataRecord.getType() == ChangeDataRecordType.RECORD_UPDATE) { // update and insert are the same type in binary
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
    // get document ID from the binary key
    String docId = Bytes.toString(changeDataRecord.getId().getBinary().array());


    // Create JSON Message for the Full Text Service
    ObjectNode indexingMessage = jsonMapper.createObjectNode();
    boolean sendIndexingMessage = false; // only send the message if at least one of the field is present
    ObjectNode fieldToIndex = jsonMapper.createObjectNode();
    indexingMessage.put("_id", docId);
    indexingMessage.put("operation", changeDataRecord.getType().toString());
    indexingMessage.put("type", "binary");


git

    if (sendIndexingMessage) {
      indexingMessage.set("fields_to_index", fieldToIndex);

      producer.send(new ProducerRecord<String, String>(
              FTS_TOPIC,
              indexingMessage.toString()));

      System.out.println("\t   Posting to FTS Service " + indexingMessage);
    }


  }


  /**
   * Send a delete message to the FTS Service topic
   *
   * @param changeDataRecord to process
   * @param producer         to post the messages created during CDC processing
   */
  private static void deleteDocument(ChangeDataRecord changeDataRecord, KafkaProducer<String, String> producer) {
    String docId = Bytes.toString(changeDataRecord.getId().getBinary().array());

    ObjectNode indexingMessage = jsonMapper.createObjectNode();
    boolean sendIndexingMessage = false; // only send the message if at least one of the field is present
    indexingMessage.put("_id", docId);
    indexingMessage.put("operation", changeDataRecord.getType().toString());

    producer.send(new ProducerRecord<String, String>(
            "/demo_app_stream:fts_service",
            indexingMessage.toString()));

    System.out.println("\t   Posting to FTS Service " + indexingMessage);

  }


}
