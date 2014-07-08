import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.BasicConfigurator;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Map;

/**
 * A lightweight JMS / ActiveMQ wrapper for producing and consuming JMS messages.
 * See JmsTest.java for example usages.
 *
 * @author Luke Terheyden (terheyden@gmail.com)
 */
public class JmsConnector {

    // Connection factory name in jndi.properties:
    protected static final String JMS_CONN_FACTORY_NAME = "connectionFactory";

    // Specific JMS URL / queue settings for this connection:
    protected boolean useJndi = true;
    protected String jmsUrl;
    protected String jmsQueueName;

    // Plumbing needed for JMS:
    protected InitialContext jndi;
    protected ConnectionFactory conFactory;
    protected Connection connection;
    protected Session session;
    protected Destination destination;
    protected MessageProducer producer;
    protected MessageConsumer consumer;

    /**
     * If you use this constructor, the JMS URL and queue name will be looked up from jndi.properties.
     * The default connection factory name is "connectionFactory".
     * You must call close() in a finally block.
     *
     * @param jmsQueueName in jndi.properties, if "queue.MyQueue = myqueue" is defined, this should be "MyQueue".
     */
    public JmsConnector(String jmsQueueName) {
        useJndi = true;
        this.jmsQueueName = jmsQueueName;
    }

    /**
     * You can use this constructor to not use JNDI and to specify the JMS URL and queue name directly.
     * You must call close() in a finally block.
     *
     * @param jmsUrl e.g. "tcp://localhost:61616"
     * @param jmsQueueName e.g. "users" or whatever direct queue name shows up in ActiveMQ
     */
    public JmsConnector(String jmsUrl, String jmsQueueName) {
        useJndi = false;
        this.jmsUrl = jmsUrl;
        this.jmsQueueName = jmsQueueName;
    }

    /**
     * Called automatically to lazy-load our JMS connection.
     * Supports using JNDI to configure, as well as a direct URL / queue name.
     */
    private void validateConnection() throws NamingException, JMSException {

        if (useJndi) {

            // USE JNDI:

            if (jndi == null) {
                // Obtain a JNDI connection.
                jndi = new InitialContext();
            }

            if (conFactory == null) {
                // Look up our JMS connection factory.
                conFactory = (ConnectionFactory) jndi.lookup(JMS_CONN_FACTORY_NAME);
            }

        } else {

            // DIRECT CONFIG:

            if (conFactory == null) {

                BasicConfigurator.configure();

                // Make a direct ActiveMQ JMS connection.
                conFactory = new ActiveMQConnectionFactory(jmsUrl);
            }
        }

        if (connection == null) {
            // Create the JMS connection.
            connection = conFactory.createConnection();
            connection.start();
        }

        if (session == null) {
            // Create the JMS session.
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        }

        if (destination == null) {
            // Establish the queue destination.
            destination = useJndi ? (Destination) jndi.lookup(jmsQueueName) : session.createQueue(jmsQueueName);
        }
    }

    /**
     * Lazy-load the JMS connection and the message producer.
     */
    protected void validateProducer() throws JMSException, NamingException {

        validateConnection();

        if (producer == null) {
            // MessageProducer is used for receiving (consuming) messages.
            producer = session.createProducer(destination);
        }
    }

    /**
     * Lazy-load the JMS connection and the message consumer.
     */
    protected void validateConsumer() throws JMSException, NamingException {

        validateConnection();

        if (consumer == null) {
            // MessageConsumer is used for receiving (consuming) messages.
            consumer = session.createConsumer(destination);
        }
    }

    /**
     * Call this to wait for a message to arrive on the queue.
     * This call blocks forever.
     * Returned Message may be of type TextMessage, MapMessage, etc.
     */
    public Message consume() throws NamingException, JMSException {

        validateConsumer();
        return consumer.receive();
    }

    /**
     * Call this to wait for a message to arrive on the queue.
     * This call blocks, giving up after [timeoutSecs] seconds.
     * Returned Message may be of type TextMessage, MapMessage, etc.
     */
    public Message consume(int timeoutSecs) throws NamingException, JMSException {

        validateConsumer();
        return consumer.receive(timeoutSecs * 1000);
    }

    /**
     * Call this to pull a message off the queue. Does not block.
     * Returns null if there's nothing in the queue.
     * Returned Message may be of type TextMessage, MapMessage, etc.
     */
    public Message consumeNoWait() throws NamingException, JMSException {

        validateConsumer();
        return consumer.receiveNoWait();
    }

    /**
     * Send a text message over the queue.
     */
    public void sendTextMessage(String text) throws NamingException, JMSException {

        validateProducer();

        TextMessage msg = session.createTextMessage(text);
        producer.send(msg);
    }

    //////////////////////////////////////////////////////////////////////
    // Methods for building and sending a MapMessage:

    protected MapMessage mapMessage;

    /**
     * A Builder-like method used to create, build, and send a MapMessage over the queue.
     * Chain calls to addMap() methods, then finish with sendMapMessage().
     * Example usage: startMapMessage().addMapString("name", "luke").sendMapMessage();
     */
    public JmsConnector startMapMessage() throws JMSException, NamingException {
        validateProducer();
        mapMessage = session.createMapMessage();
        return this;
    }

    /**
     * While creating a map message, add an entire map of strings.
     */
    public JmsConnector addMapStringMap(Map<String, String> strMap) throws JMSException {

        validateMapMessage();

        for (Map.Entry<String, String> entry : strMap.entrySet()) {
            mapMessage.setString(entry.getKey(), entry.getValue());
        }

        return this;
    }

    /**
     * While creating a map message, add a string.
     */
    public JmsConnector addMapString(String key, String val) throws JMSException {
        validateMapMessage();
        mapMessage.setString(key, val);
        return this;
    }

    /**
     * While creating a map message, add an int.
     */
    public JmsConnector addMapInt(String key, int val) throws JMSException {
        validateMapMessage();
        mapMessage.setInt(key, val);
        return this;
    }

    /**
     * Send the built map message. Must call startMapMessage() and build with addMap() methods before ending
     * with a call to this.
     * Example usage: startMapMessage().addMapString("name", "luke").sendMapMessage();
     */
    public void sendMapMessage() throws NamingException, JMSException {
        validateMapMessage();
        producer.send(mapMessage);
        mapMessage = null;
    }

    /**
     * When building a map message, make sure they're calling the build methods in order...
     */
    private void validateMapMessage() {
        if (mapMessage == null) {
            throw new RuntimeException("You must call startMapMessage() before calling addMap() methods or sendMapMessage().");
        }
    }

    // End MapMessage-specific builder / sender methods.
    //////////////////////////////////////////////////////////////////////

    /**
     * Must finally call this to clean up resources.
     * This method is idempotent.
     */
    public void close() {

        // Quietly try to close all the things.

        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                // Ignore.
            }
        }

        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                // Ignore.
            }
        }

        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                // Ignore.
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                // Ignore.
            }
        }

        if (jndi != null) {
            try {
                jndi.close();
            } catch (Exception e) {
                // Ignore.
            }
        }

        // Let's reset everything in case the caller wants to try to reconnect if something fails.
        // Nulling these out means we'll retry to lazy-load.

        jndi = null;
        conFactory = null;
        connection = null;
        session = null;
        destination = null;
        producer = null;
        consumer = null;
    }
}
