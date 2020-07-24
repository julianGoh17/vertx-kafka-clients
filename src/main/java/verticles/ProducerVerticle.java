package verticles;

import clients.Producer;

import java.util.Map;

public class ProducerVerticle extends ClientVerticle {
    private final Map<Producer, String> producerTopicMap;

    public static final String PRODUCER_PREFIX = "producer";
    public static final String MESSAGE_POSTFIX = "message";
    public static final String MESSAGE_STREAM_POSTFIX = "message-stream";
    public static final String PARTITION_POSTFIX = "partition";
    public static final String PARTITION_STREAM_POSTFIX = "partition-stream";
    public static final String TRANSACTIONAL_ID_POSTFIX = "transactional-id";
    public static final String TRANSACTIONAL_ID_STREAM_POSTFIX = "transactional-id-stream";

    public ProducerVerticle(String verticleAddress, Map<Producer, String> producerTopicMap) {
        super(String.format("%s-%s", PRODUCER_PREFIX, verticleAddress));
        this.producerTopicMap = producerTopicMap;
    }

    @Override
    public void start() {
        System.out.println(String.format("Producer Verticle '%s' Has Started", VERTICLE_ADDRESS));
        vertx.eventBus().consumer(formatAddress(MESSAGE_POSTFIX), v -> produceSingleMessage());
        vertx.eventBus().consumer(formatAddress(MESSAGE_STREAM_POSTFIX), v -> produceMessageStream());
        vertx.eventBus().consumer(formatAddress(PARTITION_POSTFIX), partition -> produceToPartition(partition.body()));
        vertx.eventBus().consumer(formatAddress(PARTITION_STREAM_POSTFIX), partition -> produceToPartitionStream(partition.body()));
        vertx.eventBus().consumer(formatAddress(TRANSACTIONAL_ID_POSTFIX), v -> produceSingleMessageWithTransactionalId());
        vertx.eventBus().consumer(formatAddress(TRANSACTIONAL_ID_STREAM_POSTFIX), v -> produceMessageStreamWithTransactionalId());
    }

    private void produceMessageStreamWithTransactionalId() {
        vertx.setPeriodic(1000, id -> produceSingleMessageWithTransactionalId());
    }

    private void produceSingleMessageWithTransactionalId() {
        producerTopicMap.keySet().forEach(producer -> producer.produceWithTransactionalId(producerTopicMap.get(producer)));
    }

    private void produceMessageStream() {
        vertx.setPeriodic(1000, id -> produceSingleMessage());
    }

    private void produceSingleMessage() {
        producerTopicMap.keySet().forEach(producer ->  producer.produceMessage(producerTopicMap.get(producer)));
    }

    private void produceToPartitionStream(Object partition) {
        vertx.setPeriodic(1000, id -> produceToPartition(partition));
    }

    private void produceToPartition(Object partition) {
        producerTopicMap.keySet().forEach(producer ->  producer.produceToPartition(producerTopicMap.get(producer), (Integer) partition));
    }
}
