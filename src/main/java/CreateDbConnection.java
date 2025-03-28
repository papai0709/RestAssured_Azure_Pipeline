import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class CreateDbConnection {

    static Properties properties = new Properties();


    public static CosmosContainer connect() throws IOException {
        System.out.println("Reading Cosmos DB configuration");
        properties.load(Files.newInputStream(Paths.get("src/main/resources/data.properties")));
        String ENDPOINT = properties.getProperty("ENDPOINT");
        String KEY = properties.getProperty("KEY");
        String DATABASE_NAME = properties.getProperty("DATABASE_NAME");
        String CONTAINER_NAME = properties.getProperty("CONTAINER_NAME");

        System.out.println("Connecting to Cosmos DB");

        // Create a new CosmosClient
        CosmosDatabase database;
        try (CosmosClient client = new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .key(KEY)
                .buildClient()) {

            // Create or get the database
            CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(DATABASE_NAME);
            database = client.getDatabase(databaseResponse.getProperties().getId());
        }

        // Create or get the container
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(CONTAINER_NAME, "/partitionKey");
        CosmosContainer container = database.getContainer(containerResponse.getProperties().getId());

        System.out.println("Connected to Cosmos DB");
        return container;

    }


    public static void readDataFromCosmosDb(CosmosContainer container) {
        System.out.println("Reading data from Cosmos DB");

        String sqlQuery = "SELECT * FROM c WHERE c.Id = 'Emp_1'";
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setPartitionKey(new PartitionKey("id"));

        CosmosPagedIterable<Object> items = container.queryItems(sqlQuery, options, Object.class);

        items.forEach(item -> System.out.println(item));
    }

    public static void main(String[] args) {
//        CreateCosmosConnection cosmosDbConnection = new CreateCosmosConnection();
        try {
            CosmosContainer container = connect();
            readDataFromCosmosDb(container);
        } catch (IOException ignored) {

        }
    }
}