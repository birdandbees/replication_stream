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
 * Created by jing on 4/6/16.
 */
public class KafkaProducer implements AvantProducer{
    private Pattern DDL_match;
    private String maprStream;
    private Pattern topicFilter;
    private Producer<String, String> producer;
    public final static Logger logger = Logger.getLogger(KafkaProducer.class);

    public KafkaProducer(String streamName, String filter)
    {

        try {
            InputStream props = Resources.getResource("producer.props").openStream();
            Properties properties = new Properties();
            properties.load(props);
            producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String> (properties);
            DDL_match = Pattern.compile("^(INSERT INTO|DELETE|UPDATE)\\s+(?<schema>\\w+).(?<table>\\w+)");
            this.maprStream = streamName;
            if ( filter != null) {
                this.topicFilter = Pattern.compile(filter);
            }else{
                topicFilter = null;
            }
        }
        catch (Exception e)
        {
            logger.fatal(e.getMessage());
            System.exit(1);
        }
    }

    public String extractTopic(String message)
    {
        String topic = null;
        Matcher m = DDL_match.matcher(message);
        if ( m.find())
        {
            topic = m.group(3);

        }
        if ( topicFilter != null && !topicFilter.matcher(topic).find() )
        {
            return null;
        }
        return topic;
    }

    private void send(Message message)
    {
        String topic = extractTopic(message.content);

        if (topic != null )
        {
            producer.send(new ProducerRecord<String, String>(
                    maprStream + ":" + topic,
                    topic + Integer.toString(message.key),
                    String.format("{\"type\":\"%s\", \"t\":%.3f}", message.content, System.nanoTime() * 1e-9)));
        }

    }

    public void push(Stream stream)
    {
        for (Message message: stream.getStringData()) {
            send(message);
        }

    }

    public void flush()
    {
        producer.flush();
    }

    public void close()
    {
        producer.close();
    }
}
