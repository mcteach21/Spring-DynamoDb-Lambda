package mc.apps.aws;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.UUID;


/**
 * customerRequest object – which will contain the request values passed in JSON format
 * Context object – used to get information from lambda execution environment
 * CustomerResponse – which is the response object for the lambda request
 */

public class SaveCustomerHandler implements RequestHandler<CustomerRequest, CustomerResponse> {
    private DynamoDB dynamoDB;
    private String DYNAMODB_TABLE_NAME = "Customer";

    private String amazonDynamoDBEndpoint = "dynamodb.eu-west-3.amazonaws.com";
    private String amazonDynamoDBRegion = "eu-west-3";

    public CustomerResponse handleRequest(CustomerRequest customerRequest, Context context) {

        initDynamoDbClient();
        persistData(customerRequest);

        CustomerResponse customerResponse = new CustomerResponse();
        customerResponse.setMessage("table "+DYNAMODB_TABLE_NAME+" Saved Successfully!");

        return customerResponse;
    }

    private PutItemOutcome persistData(CustomerRequest customerRequest) throws ConditionalCheckFailedException {

        Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
        return  table.putItem(new PutItemSpec()
                                .withItem(new Item()
                                    .withString("id", UUID.randomUUID().toString())
                                    .withString("name", customerRequest.getName())

                                    /*  .withString("pK", "PKxxxxxxxxxxxxxxxxxxxxxxx")
                                        .withString("sK","SKxxxxxxxxxxxxxxxxxxxxxxx")

                                        // entity USER
                                        .withString("address","")
                                        .withString("birthdate", "")
                                        .withString("email", "")
                                        .withString("name", "")

                                        // entity GAME
                                        .withString("map", "")
                                        .withString("create_time", "")
                                        .withString("people", "")
                                        .withString("open_timestamp", "")
                                        .withString("creator", "")

                                        // entity GAME_USER
                                        .withString("game_id", "")
                                        .withString("username", "")
                                     */
                                )
                );
    }
    private void initDynamoDbClient() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                amazonDynamoDBEndpoint, amazonDynamoDBRegion
                        )
                )
                .build();
        this.dynamoDB = new DynamoDB(client);
    }
}