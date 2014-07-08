import org.testng.Assert;
import org.testng.annotations.Test;

import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Example usages of JmsConnector.
 *
 * @author Luke Terheyden (terheyden@gmail.com)
 */
public class JmsTest {

    /**
     * Example usage where the JMS connection info is looked up from jndi.properties.
     */
    @Test
    public void testJmsJndi() {

        // Construction never throws, so it can be done outside the try..catch.
        // In jndi.properties you'll have something like: queue.MyQueue = myqueue
        // Here you want to specify the "MyQueue" part.
        JmsConnector jmsConn = new JmsConnector("UserQueue");

        try {

            // Send a text message.
            jmsConn.sendTextMessage("This is a text message!!");

            // Build and send a map message:
            jmsConn.startMapMessage()
                .addMapString("name", "luke")
                .addMapInt("age", 29)
                .sendMapMessage();

            // Consume the text message - wait forever.
            Message msg1 = jmsConn.consume();
            Assert.assertTrue(msg1 instanceof TextMessage);

            // Consume the map message - wait only 5 seconds.
            Message msg2 = jmsConn.consume(5);
            Assert.assertTrue(msg2 instanceof MapMessage);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Always close the connection in a finally block:
            jmsConn.close();
        }
    }

    /**
     * Example usage without using JNDI. JMS url and queue names are directly provided.
     */
    @Test
    public void testJmsDirect() {

        // Construction never throws, so it can be done outside the try..catch.
        // This doesn't use JNDI, so we specify the exact queue name.
        JmsConnector jmsConn = new JmsConnector("tcp://localhost:61616", "users");

        try {

            // Send a text message.
            jmsConn.sendTextMessage("This is a text message!!");

            // Build and send a map message:
            jmsConn.startMapMessage()
                .addMapString("name", "luke")
                .addMapInt("age", 29)
                .sendMapMessage();

            // Consume the text message - wait forever.
            Message msg1 = jmsConn.consume();
            Assert.assertTrue(msg1 instanceof TextMessage);

            // Consume the map message - wait only 5 seconds.
            Message msg2 = jmsConn.consume(5);
            Assert.assertTrue(msg2 instanceof MapMessage);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Always close the connection in a finally block:
            jmsConn.close();
        }
    }
}
