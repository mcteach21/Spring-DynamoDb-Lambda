package mc.apps.aws;

import lombok.Data;
import java.util.List;

@Data
public class CustomersResponse {
    List<Customer> items;

}
