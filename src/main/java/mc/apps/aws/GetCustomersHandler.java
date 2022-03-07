package mc.apps.aws;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;

import java.io.IOException;
import java.util.*;

public class GetCustomersHandler implements RequestHandler<String, CustomersResponse> {
    private DynamoDB dynamoDB;
    private static String DYNAMODB_TABLE_NAME = "Customer";

    private static String amazonDynamoDBEndpoint = "dynamodb.eu-west-3.amazonaws.com";
    private static String amazonDynamoDBRegion = "eu-west-3";

    @Override
    public CustomersResponse handleRequest(String input, Context context) {
        initDynamoDbClient();
        initObjectMapper();

        Table table = dynamoDB.getTable(DYNAMODB_TABLE_NAME);
        ScanSpec scanSpec = new ScanSpec()
                // ok
                .withFilterExpression("contains(#name,:name)")
                .withNameMap(new NameMap().with("#name", "name"))
                .withValueMap(new ValueMap()
                        .withString(":name", input)
                );


                // ok
                //.withProjectionExpression("#id")
                /*  .withFilterExpression("#id between :start_id and :end_id")
                .withNameMap(new NameMap().with("#id", "id"))
                .withValueMap(new ValueMap()
                        .withString(":start_id", "1a872987-e77f-422d-902e-c539b1394a5d")
                        .withString(":end_id", "1a872987-e77f-422d-902e-c539b1394a5d")
                );*/

       ItemCollection<ScanOutcome> items = table.scan(scanSpec);

        List<Customer> customers = new ArrayList();

        Iterator<Item> iter = items.iterator();
        Item item;
        while (iter.hasNext()) {
            item = iter.next();
            customers.add(stringToItem(item.toJSON(), Customer.class));
        }

        CustomersResponse customersResponse = new CustomersResponse();
        customersResponse.setItems(customers);
        customersResponse.setMessage("Scan result : "+customers.size()+" items!");

        return customersResponse;
    }

    private Customer stringToItem(final String item, final Class<Customer> valueType) {
        Customer value = null;
        try {
            value = mapper.readValue(item, valueType);
        } catch (final IOException e) {
            //throw new PersistenceResourceFailureException("Failure converting String to item", e);
        }
        return value;
    }

    private ObjectMapper mapper;
    public void initObjectMapper() {

        mapper = new ObjectMapper();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.registerModule(new JodaModule());
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
