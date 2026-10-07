package org.kiwiproject.dynamodb.leader;

import static java.util.Objects.nonNull;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.net.ServerSocket;
import java.net.URI;

/**
 * Starts an in-memory, embedded DynamoDB Local (no Docker required) for the duration of a test class.
 */
class DynamoDbLocalExtension implements BeforeAllCallback, AfterAllCallback {

    static final String TABLE_NAME = "service-leader-locks";

    private static DynamoDBProxyServer server;
    private static int port;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        port = freePort();
        server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", String.valueOf(port)});
        server.start();

        try (var client = newClient()) {
            client.createTable(CreateTableRequest.builder()
                    .tableName(TABLE_NAME)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName(AwsLockGateway.PARTITION_KEY_NAME)
                            .attributeType(ScalarAttributeType.S)
                            .build())
                    .keySchema(KeySchemaElement.builder()
                            .attributeName(AwsLockGateway.PARTITION_KEY_NAME)
                            .keyType(KeyType.HASH)
                            .build())
                    .build());
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (nonNull(server)) {
            server.stop();
        }
    }

    static DynamoDbClient newClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:" + port))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .build();
    }

    private static int freePort() throws Exception {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
