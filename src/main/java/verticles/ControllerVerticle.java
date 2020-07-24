package verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;

import java.util.List;

import static verticles.ConsumerVerticle.CONSUMER_PREFIX;
import static verticles.ProducerVerticle.PARTITION_POSTFIX;
import static verticles.ProducerVerticle.PARTITION_STREAM_POSTFIX;
import static verticles.ProducerVerticle.PRODUCER_PREFIX;
import static verticles.ProducerVerticle.TRANSACTIONAL_ID_POSTFIX;
import static verticles.ProducerVerticle.TRANSACTIONAL_ID_STREAM_POSTFIX;

public class ControllerVerticle extends AbstractVerticle {
    private static final String CONTROLLER_VERTICLE_NAME = "Controller";
    private final List<String> producerVerticleNames;
    private final List<String> consumerVerticleNames;
    private Vertx vertx;

    public ControllerVerticle(List<String> producerVerticleNames, List<String> consumerVerticleNames, Vertx vertx) {
        this.producerVerticleNames = producerVerticleNames;
        this.consumerVerticleNames = consumerVerticleNames;
        this.vertx = vertx;
    }

    @Override
    public void start() {
        System.out.println(String.format("%s Verticle has started!", CONTROLLER_VERTICLE_NAME));
    }

    public void consumeFromAllVerticles() {
        consumerVerticleNames.forEach(this::consumeFromVerticle);
    }

    public void consumeFromVerticle(String name) {
        sendToVerticle(getConsumerVerticleAddress(name, ConsumerVerticle.CONSUME_POSTFIX));
    }

    /**
     * Producer Methods
     */
    public void produceSingleMessageToAllVerticles() {
        producerVerticleNames.forEach(this::produceSingleMessageToVerticle);
    }

    private void produceSingleMessageToVerticle(String name) {
        sendToVerticle(getProducerVerticleAddress(name, ProducerVerticle.MESSAGE_POSTFIX));
    }

    public void produceMessageStreamToAllVerticles() {
        producerVerticleNames.forEach(this::produceMessageStreamToVerticle);
    }

    private void produceMessageStreamToVerticle(String name) {
        sendToVerticle(getProducerVerticleAddress(name, ProducerVerticle.MESSAGE_STREAM_POSTFIX));
    }

    public void produceMessageToSinglePartitionToAllVerticles(int partition) {
        producerVerticleNames.forEach(name -> produceMessageToSinglePartitionToVerticle(name, partition));
    }

    public void produceMessageToSinglePartitionToVerticle(String name, int partition) {
        sendToVerticleWithPayload(getProducerVerticleAddress(name, PARTITION_POSTFIX), partition);
    }

    public void produceMessageStreamToSinglePartitionToAllVerticles(int partition) {
        producerVerticleNames.forEach(name -> produceMessageStreamToSinglePartitionToVerticle(name, partition));
    }

    public void produceMessageStreamToSinglePartitionToVerticle(String name, int partition) {
        sendToVerticleWithPayload(getProducerVerticleAddress(name, PARTITION_STREAM_POSTFIX), partition);
    }

    public void produceMessageWithTransactionalIdToAllVerticles() {
        producerVerticleNames.forEach(this::produceMessageWithTransactionalIdToVerticle);
    }

    private void produceMessageWithTransactionalIdToVerticle(String name) {
        sendToVerticle(getProducerVerticleAddress(name, TRANSACTIONAL_ID_POSTFIX));
    }

    public void produceMessageStreamWithTransactionalIdToAllVerticles() {
        producerVerticleNames.forEach(this::produceMessageStreamWithTransactionalIdToVerticle);
    }

    private void produceMessageStreamWithTransactionalIdToVerticle(String name) {
        sendToVerticle(getProducerVerticleAddress(name, TRANSACTIONAL_ID_STREAM_POSTFIX));
    }

    /**
     * Utility Functions
     */
    private void sendToVerticle(String address) {
        sendToVerticleWithPayload(address, "random-message");
    }

    private void sendToVerticleWithPayload(String address, Object payload) {
        vertx.eventBus().send(address, payload);
    }

    private String getProducerVerticleAddress(String name, String postfix) {
        return String.format("%s-%s-%s", PRODUCER_PREFIX, name, postfix);
    }

    private String getConsumerVerticleAddress(String name, String postfix) {
        return String.format("%s-%s-%s", CONSUMER_PREFIX, name, postfix);
    }
}
