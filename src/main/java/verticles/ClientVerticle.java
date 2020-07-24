package verticles;

import io.vertx.core.AbstractVerticle;

public abstract class ClientVerticle extends AbstractVerticle {
    protected final String VERTICLE_ADDRESS;

    public ClientVerticle(String verticleAddress) {
        this.VERTICLE_ADDRESS = verticleAddress;
    }

    protected String formatAddress(String postfix) {
        return String.format("%s-%s", VERTICLE_ADDRESS, postfix);
    }
}
