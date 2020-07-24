package models;

public enum  ClientType {
    PRODUCER("producer"),
    CONSUMER("consumer");

    private String value;

    ClientType(String value) {
        this.value = value;
    }

    public String toValue() {
        return this.value;
    }
}
