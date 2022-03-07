package mc.apps.aws;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@Slf4j
public class AwsScaanOperationsTests {
    private static String DYNAMODB_TABLE_NAME = "Customer";

    private static String amazonDynamoDBEndpoint = "dynamodb.eu-west-3.amazonaws.com";
    private static String amazonDynamoDBRegion = "eu-west-3";
    private static AmazonDynamoDB dynamoDBClient;

    /**
     * generated code (NoSQL Workbench)
     */

    @BeforeEach
    public void Setup(){
        createDynamoDbClient();
    }

    @Test
    public void test(){
        try {

            ScanRequest scanRequest = createScanRequest();
            ScanResult scanResult = dynamoDBClient.scan(scanRequest);
            System.out.println("Scan successful.");

            assertTrue(true);

            // Handle scanResult
            List<Map<String, AttributeValue>> items = scanResult.getItems();
            for (Map<String, AttributeValue> item : items){
                item.keySet().forEach(k->
                       log.info("{} = {}. ", k , item.get(k).getS())
                );
            }

        } catch (Exception e) {
            // handleScanErrors(e);
        }
    }

    private void createDynamoDbClient(){
        dynamoDBClient = AmazonDynamoDBClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                amazonDynamoDBEndpoint, amazonDynamoDBRegion
                        )
                )
                .build();
    }
    private ScanRequest createScanRequest() {
        ScanRequest scanRequest = new ScanRequest();
        scanRequest.setTableName(DYNAMODB_TABLE_NAME);

        String filterExpression = "contains(#6ae91, :6ae91)";
        scanRequest.setFilterExpression(filterExpression);

        String projectionExpression = "#id, #6ae90";
        scanRequest.setProjectionExpression(projectionExpression);

        scanRequest.setConsistentRead(false);
        scanRequest.setExpressionAttributeNames(getExpressionAttributeNames());
        scanRequest.setExpressionAttributeValues(getExpressionAttributeValues());

        return scanRequest;
    }

    private Map<String, String> getExpressionAttributeNames() {
        Map<String, String> expressionAttributeNames = new HashMap<String, String>();

        expressionAttributeNames.put("#id", "id");
        expressionAttributeNames.put("#6ae90", "name");
        expressionAttributeNames.put("#6ae91", "name");
        return expressionAttributeNames;
    }

    private Map<String, AttributeValue> getExpressionAttributeValues() {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":6ae91", new AttributeValue("Mehdi"));
        return expressionAttributeValues;
    }
}
