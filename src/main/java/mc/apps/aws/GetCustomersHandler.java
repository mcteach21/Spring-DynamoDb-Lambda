package mc.apps.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class GetCustomersHandler implements RequestHandler<Void, CustomersResponse> {

    @Override
    public CustomersResponse handleRequest(Void unused, Context context) {

        return null;
    }
}
