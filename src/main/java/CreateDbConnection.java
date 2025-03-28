import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedFlux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class CreateDbConnection {

    static Properties properties = new Properties();
    static CosmosAsyncClient client;

    public static CosmosAsyncContainer connect() throws IOException {
        System.out.println("Reading Cosmos DB configuration");
        properties.load(Files.newInputStream(Paths.get("src/main/resources/data.properties")));
        String ENDPOINT = properties.getProperty("ENDPOINT");
        String KEY = properties.getProperty("KEY");
        String DATABASE_NAME = properties.getProperty("DATABASE_NAME");
        String CONTAINER_NAME = properties.getProperty("CONTAINER_NAME");

        System.out.println("Connecting to Cosmos DB");

        // Create a new CosmosAsyncClient
        client = new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .key(KEY)
                .buildAsyncClient();

        // Create or get the database
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(DATABASE_NAME).block();
        assert databaseResponse != null;
        CosmosAsyncDatabase database = client.getDatabase(databaseResponse.getProperties().getId());

        // Create or get the container
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(CONTAINER_NAME, "/partitionKey").block();
        assert containerResponse != null;
        CosmosAsyncContainer container = database.getContainer(containerResponse.getProperties().getId());

        System.out.println("Connected to Cosmos DB");
        return container;
    }

    public static void readDataFromCosmosDb(@org.jetbrains.annotations.NotNull CosmosAsyncContainer container) {
        System.out.println("Reading data from Cosmos DB");

        String sqlQuery = "SELECT * FROM c WHERE c.id = 'Emp_1001'";
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();

        CosmosPagedFlux<Object> items = container.queryItems(sqlQuery, options, Object.class);
        items.byPage().subscribe(page -> {
            System.out.println("Data read from Cosmos DB");
            page.getElements().forEach(System.out::println);
            closeConnection();
        });
    }

    public static void closeConnection() {
        if (client != null) {
            client.close();
            System.out.println("Connection to Cosmos DB closed");
        }
    }

    public static void main(String[] args) {
        try {
            CosmosAsyncContainer container = connect();
            readDataFromCosmosDb(container);
        } catch (IOException ignored) {
            System.out.println("Error reading properties file");
        } catch (Exception e) {
            System.out.println("Error connecting to Cosmos DB: " + e.getMessage());
        } finally {
            System.out.println("Finished");
        }
    }
}