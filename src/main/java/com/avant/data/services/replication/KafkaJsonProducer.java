package com.avant.data.services.replication;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Created by jing on 4/22/16.
 */
public class KafkaJsonProducer implements AvantProducer{

    private Producer<String, String> producer;
    public final static Logger logger = Logger.getLogger(KafkaJsonProducer.class);
    public String maprStream;
    private Pattern topicFilter;
    private Pattern topic_match;
    public KafkaJsonProducer(String streamName, String filter) {

        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            topic_match = Pattern.compile("\"table\":\"(?<table>\\w+)\"");
            producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
            this.maprStream = streamName;
            if (filter != null) {
                this.topicFilter = Pattern.compile(filter);
            } else {
                topicFilter = null;
            }
        } catch (Exception e) {
            logger.fatal(e.getMessage());
            System.exit(1);
        }
    }


    public String extractTopic(String message) {
        String topic = null;
        Matcher m = topic_match.matcher(message);
        if (m.find()) {
            topic = m.group(1);

        }
        if (topicFilter != null && !topicFilter.matcher(topic).find()) {
            return null;
        }
        return topic;
    }

    private void send(Message message) {
        String topic = extractTopic(message.content);

        if (topic != null) {
            producer.send(new ProducerRecord<String, String>(
                    maprStream + ":" + topic,
                    topic + Integer.toString(message.key),
                    String.format("%s", message.content)));
        }

    }


    public void push(Stream stream) {
        int last_key = 0;
        String jsonString = "";
        for (Message message : stream.getStringData()) {
            if ( last_key != 0 && last_key != message.key)
            {
                Message parsedMessage = new Message();
                parsedMessage.key = last_key;
                parsedMessage.content = jsonString;
                send(parsedMessage);
                jsonString = "";
            }else{
                jsonString += message.content;
            }
        }

    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }

}
