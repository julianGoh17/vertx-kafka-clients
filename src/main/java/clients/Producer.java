package clients;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import models.ClientAuthenticationType;
import models.ClientType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class Producer {
    private static final ClientType clientType = ClientType.PRODUCER;

    private ClientAuthenticationType clientAuthenticationType;
    private String name;
    private Properties properties;
    private KafkaProducer<String, String> producer;
    private AtomicInteger messageNumber = new AtomicInteger(1);

    private Producer(String name, ClientAuthenticationType clientAuthenticationType, Properties properties) {
        this.name = name;
        this.clientAuthenticationType = clientAuthenticationType;
        this.properties = properties;
        this.properties.put("client.id", name);
    }

    public void produceMessage(String topic) {
        String messageContent = getMessageContent();
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topic, messageContent);
        produce(record, messageContent);
    }

    public void produceToPartition(String topic, Integer partition) {
        String messageContent = getMessageContent();
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topic, null, messageContent, partition);
        produce(record, messageContent);
    }

    public void produceWithTransactionalId(String topic) {
        String messageContent = String.format("%s with transactional id '%s'", getMessageContent(), properties.getProperty("transactional.id"));
        org.apache.kafka.clients.producer.Producer<String, String> apacheKafkaProducer = producer.unwrap();
        apacheKafkaProducer.initTransactions();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageContent);

        try {
            System.out.println("Sending Message: " + messageContent);
            apacheKafkaProducer.beginTransaction();
            apacheKafkaProducer.send(record);
            apacheKafkaProducer.commitTransaction();
            System.out.println("Sent Message: " + messageContent);
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            System.out.println(String.format("Kafka Producer '%s' had to close due to error: %s", name, e.getMessage()));
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            apacheKafkaProducer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println(String.format("Kafka producer '%s' aborted transaction due to: %s", name, e.getMessage()));
            apacheKafkaProducer.abortTransaction();
        }
        apacheKafkaProducer.close();
    }

    private void produce(KafkaProducerRecord<String, String> record, String messageContent) {
        producer.send(record, done -> {
            if (done.succeeded()) {
                RecordMetadata recordMetadata = done.result();
                System.out.printf("Message: \"%s\" has been sent to topic '%s' on partition %d and offset %d by %s %s\n",
                    messageContent, recordMetadata.getTopic(), recordMetadata.getPartition(), recordMetadata.getOffset(), clientAuthenticationType.toValue(), clientType.toValue());
            } else {
                System.out.printf("....Failed to send message '%s' because: %s\n", messageContent, done.cause());
            }
        });
    }

    private String getMessageContent() {
        return String.format("%s %s '%s' message '%d'", clientAuthenticationType.toValue(), clientType.toValue(), name, messageNumber.getAndIncrement());
    }

    /*
     * Set Up Methods
     */

    public void initializeVertxProducer(Vertx vertx) {
        this.producer = KafkaProducer.create(vertx, properties);
    }

    public void addTransactionalIdProperty(String transactionalId) {
        properties.put("transactional.id", transactionalId);
    }

    public void addAcks(String acks) {
        properties.put("acks", acks);
    }

    public void addAdditionalProperty(String key, String value) {
        this.properties.put(key, value);
    }

    /**
     * Generate Producer Methods
     */

    public static Producer createScramProducer(String name, String bootstrap, String truststorePath, String truststorePassword, String username, String password) {
        return new Producer(
            name,
            ClientAuthenticationType.SCRAM,
            ClientProperties.getScramProperties(bootstrap, truststorePath, truststorePassword, username, password, clientType));
    }

    public static Producer createMutualTlsProducer(String name, String bootstrap, String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) {
        return new Producer(
            name,
            ClientAuthenticationType.TLS,
            ClientProperties.getTlsProperties(bootstrap, truststorePath, truststorePassword, keystorePath, keystorePassword, clientType));
    }

    public static Producer createUnauthenticatedProducer(String name, String bootstrap, String truststorePath, String truststorePassword) {
        return new Producer(
            name,
            ClientAuthenticationType.UNAUTHENTICATED,
            ClientProperties.getUnauthenticatedProperties(bootstrap, truststorePath, truststorePassword, clientType));
    }

    public static Producer createHelmEventStreamsProducer(String name, String bootstrap, String truststorePath, String truststorePassword, String username, String apiKey) {
        return new Producer(
            name,
            ClientAuthenticationType.HELM_EVENT_STREAMS,
            ClientProperties.getHelmEventStreamsProperties(bootstrap, truststorePath, truststorePassword, username, apiKey, clientType));
    }
}
