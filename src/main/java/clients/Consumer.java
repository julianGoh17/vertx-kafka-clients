package clients;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import models.ClientAuthenticationType;
import models.ClientType;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Consumer {
    private static final ClientType clientType = ClientType.CONSUMER;

    private String name;
    private ClientAuthenticationType clientAuthenticationType;
    private Properties properties;
    private KafkaConsumer<String, String> consumer;

    private Consumer(String name, ClientAuthenticationType clientAuthenticationType, Properties properties) {
        this.clientAuthenticationType = clientAuthenticationType;
        this.properties = properties;
        this.name = name;
    }

    public void consumeFromTopic(String topic) {
        System.out.println(String.format("Consumer '%s' beginning", name));
        consumer.handler(message -> {
            String outcome = String.format("Consumed from topic '%s' and the message is '%s' from offset '%s' and partition '%s'\n",
                message.topic(), message.value(), message.offset(), message.partition());
            System.out.println(outcome);
        });

        consumer.subscribe(topic, outcome -> {
            if (outcome.succeeded()) {
                System.out.println(String.format("%s %s '%s' was able to subscribe to '%s'", clientAuthenticationType.toValue(), clientType.toValue(), name, topic));
            } else {
                System.out.println(String.format("...ERROR %s %s '%s' could not subscribe because %s", clientAuthenticationType.toValue(), clientType.toValue(), name, outcome.cause().getMessage()));
            }
        });
    }

    /*
     * Set Up Methods
     */
    public void initializeConsumer(Vertx vertx) {
        addConsumerGroupProperty(String.format("%s-group", name));
        this.consumer = KafkaConsumer.create(vertx, properties);
    }

    public void addConsumeFromEarliestOffset() {
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private void addConsumerGroupProperty(String group) {
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    }

    public void addAdditionalProperties(Properties properties) {
        this.properties.putAll(properties);
    }

    /**
     * Generate Consumer Methods
     */

    public static Consumer createScramConsumer(String name, String bootstrap, String truststorePath, String truststorePassword, String username, String password) {
        return new Consumer(
            name,
            ClientAuthenticationType.SCRAM,
            ClientProperties.getScramProperties(bootstrap, truststorePath, truststorePassword, username, password, clientType));
    }

    public static Consumer createMutualTlsConsumer(String name, String bootstrap, String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) {
        return new Consumer(
            name,
            ClientAuthenticationType.TLS,
            ClientProperties.getTlsProperties(bootstrap, truststorePath, truststorePassword, keystorePath, keystorePassword, clientType));
    }

    public static Consumer createUnauthenticatedConsumer(String name, String bootstrap, String truststorePath, String truststorePassword) {
        return new Consumer(
            name,
            ClientAuthenticationType.UNAUTHENTICATED,
            ClientProperties.getUnauthenticatedProperties(bootstrap, truststorePath, truststorePassword, clientType));
    }

    public static Consumer createHelmEventStreamsConsumer(String name, String bootstrap, String truststorePath, String truststorePassword, String username, String apiKey) {
        return new Consumer(
            name,
            ClientAuthenticationType.HELM_EVENT_STREAMS,
            ClientProperties.getHelmEventStreamsProperties(bootstrap, truststorePath, truststorePassword, username, apiKey, clientType));
    }
}
