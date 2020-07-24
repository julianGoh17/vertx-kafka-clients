package clients;

import models.ClientType;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Properties;

public class ClientProperties {
    public static Properties getScramProperties(String bootstrap, String truststorePath, String truststorePassword, String username, String password, ClientType clientType) {
        Properties properties = getUnauthenticatedProperties(bootstrap, truststorePath, truststorePassword, clientType);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, String.format( "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", username, password));
        return properties;
    }

    public static Properties getTlsProperties(String bootstrap, String truststorePath, String truststorePassword, String keystorePath, String keystorePassword, ClientType clientType) {
        Properties properties = getUnauthenticatedProperties(bootstrap, truststorePath, truststorePassword, clientType);
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
        properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keystorePath);
        properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        return properties;
    }

    public static Properties getHelmEventStreamsProperties(String bootstrap, String truststorePath, String truststorePassword, String username, String apiKey, ClientType clientType) {
        Properties properties = getUnauthenticatedProperties(bootstrap, truststorePath, truststorePassword, clientType);
        properties.put("security.protocol", "SASL_SSL");
        properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        properties.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";", username, apiKey));
        properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
        return properties;
    }

    public static Properties getUnauthenticatedProperties(String bootstrap, String truststorePath, String truststorePassword, ClientType clientType) {
        Properties properties = new Properties();
        generateCommonProperties(properties, bootstrap, clientType);
        generateTlsProperties(properties, truststorePath, truststorePassword);
        return properties;
    }

    private static void generateCommonProperties(Properties properties, String bootstrap, ClientType clientType) {
        if (ClientType.PRODUCER.equals(clientType)) {
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        } else {
            properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    }

    private static void generateTlsProperties(Properties properties, String truststorePath, String truststorePassword) {
        properties.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12");
        properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
        properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
    }
}
