package edu.unc.mapseq.messaging;

import java.io.IOException;
import java.io.StringWriter;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class CASAVAMessageTest {

    @Test
    public void testCAVASAQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616", "152.54.3.109"));
        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncnexus.casava");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "FileData");
            generator.writeStringField("id", "801295");
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", "161107_UNC21_0368_000000000-AWBH2_CASAVA");

            generator.writeArrayFieldStart("attributes");

            generator.writeStartObject();
            generator.writeStringField("name", "allowMismatches");
            generator.writeStringField("value", "false");
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("name", "barcodeLength");
            generator.writeStringField("value", "10");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();
            System.out.println(sw.toString());

            producer.send(session.createTextMessage(sw.toString()));

        } catch (IOException | JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

    }

    @Test
    public void testJSON() {

        try {
            StringWriter sw = new StringWriter();

            JsonGenerator generator = new JsonFactory().createGenerator(sw);

            generator.writeStartObject();
            generator.writeArrayFieldStart("entities");

            generator.writeStartObject();
            generator.writeStringField("entityType", "FileData");
            generator.writeStringField("id", "775487");
            generator.writeEndObject();

            generator.writeStartObject();
            generator.writeStringField("entityType", "WorkflowRun");
            generator.writeStringField("name", "150714_UNC16-SN851_0572_BH5N2KBCXX_CASAVA");

            generator.writeArrayFieldStart("attributes");

            generator.writeStartObject();
            generator.writeStringField("name", "allowMismatches");
            generator.writeStringField("value", "false");
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.writeEndArray();
            generator.writeEndObject();

            generator.flush();
            generator.close();

            sw.flush();
            sw.close();
            System.out.println(sw.toString());

            String format = "{\"entities\":[{\"entityType\":\"FileData\",\"id\":\"%d\"},{\"entityType\":\"WorkflowRun\",\"name\":\"%s\"}]}";
            System.out.println(String.format(format, 775487, "150714_UNC16-SN851_0572_BH5N2KBCXX_CASAVA"));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
