package models;

public enum ClientAuthenticationType {
    TLS("tls"),
    SCRAM("scram-sha-512"),
    HELM_EVENT_STREAMS("helm-event-streams"),
    UNAUTHENTICATED("no-auth");

    private String value;

    ClientAuthenticationType(String value) {
        this.value = value;
    }

    public String toValue() {
        return this.value;
    }
}
