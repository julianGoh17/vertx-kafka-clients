import clients.Consumer;
import clients.Producer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import verticles.ConsumerVerticle;
import verticles.ControllerVerticle;
import verticles.ProducerVerticle;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Application {
    private static final String SCRAM_USERNAME = "all-user";
    private static final String SCRAM_PASSWORD = "J7hYWWdbBFoq";
    private static final String HELM_USERNAME = "any";
    private static final String HELM_API_KEY = "rHIN1XGWpYoQYHGLc8540iLR3mhgtaCDJu6beMvrWl_B";
    private static final String KEYSTORE_PATH = "/Users/juliangoh/Downloads/any-user/user.p12";
    private static final String KEYSTORE_PASS = "GIRnkF2QfWou";

    public static void main(String[] args) {
        String bootstrap = "9.30.215.148:30425";
        String truststorePath = "/Users/juliangoh/Downloads/es-cert.jks";
        String truststorePassword = "password";
        String topic = "test-topic";

        Vertx vertx = Vertx.vertx();

        List<String> verticleNames = Collections.singletonList("first");
        ControllerVerticle controllerVerticle = new ControllerVerticle(verticleNames, verticleNames, vertx);
        ProducerVerticle producerVerticle = new ProducerVerticle(verticleNames.get(0), getProducerMap(bootstrap, truststorePath, truststorePassword, topic, vertx));
        ConsumerVerticle consumerVerticle = new ConsumerVerticle(verticleNames.get(0), getConsumerMap(bootstrap, truststorePath, truststorePassword, topic, vertx));

        CompositeFuture.all(
            deployHelper(controllerVerticle, vertx),
            deployHelper(producerVerticle, vertx),
            deployHelper(consumerVerticle, vertx))
                .onSuccess(v -> {
                    System.out.println("All Verticles have been deployed.");
                    controllerVerticle.produceMessageStreamToAllVerticles();
                    controllerVerticle.consumeFromAllVerticles();
                })
                .onFailure(throwable -> System.out.println(throwable.getMessage()));
    }

    private static <T extends AbstractVerticle> Future<T> deployHelper(T verticle, Vertx vertx) {
        Promise<T> deployment = Promise.promise();
        vertx.deployVerticle(verticle, result -> {
            if (result.succeeded()) {
                deployment.complete();
            } else {
                deployment.fail(String.format("Could not deploy %s", deployment));
            }
        });
        return deployment.future();
    }

    private static Map<Producer, String> getProducerMap(String bootstrap, String truststorePath, String truststorePassword, String topic, Vertx vertx) {
        // SCRAM Credentials
//        Producer producer = Producer.createScramProducer("first", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Producer secondProducer = Producer.createScramProducer("second", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Producer thirdProducer = Producer.createScramProducer("third", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Producer fourthProducer = Producer.createScramProducer("forth", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Producer fifthProducer = Producer.createScramProducer("fifth", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Producer sixthProducer = Producer.createScramProducer("sixth", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);

        // TLS Credentials
//        Producer producer = Producer.createMutualTlsProducer(String.format("first-%s", topic), bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Producer secondProducer = Producer.createMutualTlsProducer(String.format("second-%s", topic), bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Producer thirdProducer = Producer.createMutualTlsProducer(String.format("third-%s", topic), bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Producer fourthProducer = Producer.createMutualTlsProducer("fourth", bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Producer fifthProducer = Producer.createMutualTlsProducer("fifth", bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Producer sixthProducer = Producer.createMutualTlsProducer("sixth", bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);

//        producer.addTransactionalIdProperty("test-id");

        // Unauthenticated
//        Producer producer = Producer.createUnauthenticatedProducer("cruise-control", bootstrap, truststorePath, truststorePassword);
        Producer producer = Producer.createUnauthenticatedProducer("first", bootstrap, truststorePath, truststorePassword);
//        Producer secondProducer = Producer.createUnauthenticatedProducer("second", bootstrap, truststorePath, truststorePassword);
//        Producer thirdProducer = Producer.createUnauthenticatedProducer("third", bootstrap, truststorePath, truststorePassword);
//        Producer fourthProducer = Producer.createUnauthenticatedProducer("fourth", bootstrap, truststorePath, truststorePassword);
//        Producer fifthProducer = Producer.createUnauthenticatedProducer("fifth", bootstrap, truststorePath, truststorePassword);
//        Producer sixthProducer = Producer.createUnauthenticatedProducer("sixth", bootstrap, truststorePath, truststorePassword);

        // Helm
        Producer producer = Producer.createHelmEventStreamsProducer("first", bootstrap, truststorePath, truststorePassword, HELM_USERNAME, HELM_API_KEY);
        Producer secondProducer = Producer.createHelmEventStreamsProducer("second", bootstrap, truststorePath, truststorePassword, HELM_USERNAME, HELM_API_KEY);
        Producer thirdProducer = Producer.createHelmEventStreamsProducer("third", bootstrap, truststorePath, truststorePassword, HELM_USERNAME, HELM_API_KEY);
        Producer fourthProducer = Producer.createHelmEventStreamsProducer("forth", bootstrap, truststorePath, truststorePassword, HELM_USERNAME, HELM_API_KEY);
        Producer fifthProducer = Producer.createHelmEventStreamsProducer("fifth", bootstrap, truststorePath, truststorePassword, HELM_USERNAME, HELM_API_KEY);
        Producer sixthProducer = Producer.createHelmEventStreamsProducer("sixth", bootstrap, truststorePath, truststorePassword, HELM_USERNAME, HELM_API_KEY);

        producer.initializeVertxProducer(vertx);
        secondProducer.initializeVertxProducer(vertx);
        thirdProducer.initializeVertxProducer(vertx);
        fourthProducer.initializeVertxProducer(vertx);
        fifthProducer.initializeVertxProducer(vertx);
        sixthProducer.initializeVertxProducer(vertx);
//        fourthProducer.initializeVertxProducer(vertx);

        HashMap<Producer, String> producerTopicMap = new HashMap<>();
        producerTopicMap.put(producer, topic);
        producerTopicMap.put(secondProducer, topic);
//        producerTopicMap.put(thirdProducer, topic);
//        producerTopicMap.put(fourthProducer, topic);
//        producerTopicMap.put(fifthProducer, topic);
//        producerTopicMap.put(sixthProducer, topic);

        return producerTopicMap;
    }

    private static Map<Consumer, String> getConsumerMap(String bootstrap, String truststorePath, String truststorePassword, String topic, Vertx vertx) {
        // SCRAM Credentials
//        Consumer consumer = Consumer.createScramConsumer("first", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Consumer secondConsumer = Consumer.createScramConsumer("second", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
//        Consumer thirdConsumer = Consumer.createScramConsumer("third", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);

        // TLS Credentials
//        Consumer consumer = Consumer.createMutualTlsConsumer("first", bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Consumer secondConsumer = Consumer.createMutualTlsConsumer("second", bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//        Consumer thirdConsumer = Consumer.createMutualTlsConsumer("third", bootstrap, truststorePath, truststorePassword, KEYSTORE_PATH, KEYSTORE_PASS);
//
        // Unauthenticated
//        Consumer consumer = Consumer.createUnauthenticatedConsumer("test", bootstrap, truststorePath, truststorePassword);
//        Consumer secondConsumer = Consumer.createUnauthenticatedConsumer("second", bootstrap, truststorePath, truststorePassword);
//        Consumer thirdConsumer = Consumer.createUnauthenticatedConsumer("third", bootstrap, truststorePath, truststorePassword);

        // Helm
        Consumer consumer = Consumer.createHelmEventStreamsConsumer("test", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
        Consumer secondConsumer = Consumer.createHelmEventStreamsConsumer("test", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);
        Consumer thirdConsumer = Consumer.createHelmEventStreamsConsumer("test", bootstrap, truststorePath, truststorePassword, SCRAM_USERNAME, SCRAM_PASSWORD);

        consumer.addConsumeFromEarliestOffset();
        secondConsumer.addConsumeFromEarliestOffset();
        thirdConsumer.addConsumeFromEarliestOffset();

        consumer.initializeConsumer(vertx);
        secondConsumer.initializeConsumer(vertx);
        thirdConsumer.initializeConsumer(vertx);

        HashMap<Consumer, String> consumerMap = new HashMap<>();
        consumerMap.put(consumer, topic);
        consumerMap.put(secondConsumer, topic);
        consumerMap.put(thirdConsumer, topic);

        return consumerMap;
    }
}
