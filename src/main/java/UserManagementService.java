import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.bson.Document;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class UserManagementService {
    private static final String QUEUE_NAME = "user_management_queue";
    private static final String DB_NAME = "car_rental";
    private static final String CUSTOMERS_COLLECTION = "customers_tbl";

    private Connection connection;
    private Channel channel;
    private MongoDatabase database;
    private MongoCollection<Document> customersCollection;

    public UserManagementService() throws IOException, TimeoutException {
        // Connect to RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        // Connect to MongoDB
        MongoClientURI uri = new MongoClientURI("mongodb://localhost:27017");
        MongoClient mongoClient = new MongoClient(uri);
        database = mongoClient.getDatabase(DB_NAME);
        customersCollection = database.getCollection(CUSTOMERS_COLLECTION);
    }

    public void start() throws IOException {
        System.out.println("User management service started. Waiting for messages...");

        // Start consuming messages from the queue
        channel.basicConsume(QUEUE_NAME, true, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("Received message: " + message);

            // Parse the message
            String[] parts = message.split(":");
            String action = parts[0];
            String email = parts[1];
            String password = parts[2];
            String name = null;

            if (parts.length > 3) {
                name = parts[3];
            }

            // Perform the appropriate action
            switch (action) {
                case "REGISTER":
                    register(email, password, name);
                    break;
                case "LOGIN":
                    login(email, password);
                    break;
                case "UPDATE":
                    update(email, password, name);
                    break;
                case "DELETE":
                    delete(email, password);
                    break;
            }
        }, consumerTag -> {});
    }

    public void stop() throws IOException, TimeoutException {
        channel.close();
        connection.close();
        database.drop();
    }

    private void register(String email, String password, String name) {
        Document document = new Document("email", email)
                .append("password", password)
                .append("name", name);
        customersCollection.insertOne(document);
        System.out.println("User registered: " + email);
    }

    private void login(String email, String password) {
        Document query = new Document("email", email)
                .append("password", password);
        Document result = customersCollection.find(query).first();
        if (result == null) {
            System.out.println("Invalid email or password: " + email);
        } else {
            System.out.println("User logged in: " + email);
        }
    }

    private void update(String email, String password, String name) {
        Document query = new Document("email", email)
                .append("password", password);
        Document result = customersCollection.find(query).first();
        if (result == null) {
            System.out.println("Invalid email or password: " + email);
        } else {
            customersCollection.updateOne(query, new Document("$set", new Document("name", name)));
            System.out.println("User updated: " + email);
        }
    }
    private void delete(String email, String password) {
        Document query = new Document("email", email)
                .append("password", password);
        customersCollection.deleteOne(query);
        System.out.println("User deleted: " + email);
    }

    public static void main(String[] args) {
        try {
            UserManagementService service = new UserManagementService();
            service.start();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }
}

