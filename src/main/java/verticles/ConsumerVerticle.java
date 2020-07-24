package verticles;

import clients.Consumer;

import java.util.Map;

public class ConsumerVerticle extends ClientVerticle {
    private final Map<Consumer, String> consumerTopicMap;

    public final static String CONSUMER_PREFIX = "consumer";
    public final static String CONSUME_POSTFIX = "consume";

    public ConsumerVerticle(String verticleAddress, Map<Consumer, String> consumerTopicMap) {
        super(String.format("%s-%s", CONSUMER_PREFIX, verticleAddress));
        this.consumerTopicMap = consumerTopicMap;
    }

    @Override
    public void start() {
        System.out.println(String.format("Consumer Verticle '%s' Has Started", VERTICLE_ADDRESS));
        vertx.eventBus().consumer(formatAddress(CONSUME_POSTFIX), message -> allConsumersConsume());
    }

    private void allConsumersConsume() {
        consumerTopicMap.keySet().forEach(consumer -> consumer.consumeFromTopic(consumerTopicMap.get(consumer)));
    }
}


