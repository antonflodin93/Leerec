import POJO.Transaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import org.apache.commons.collections.map.HashedMap;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.*;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.lang.System.currentTimeMillis;

public class Main {
    private static int MIN_VALUE_RATING = 1;
    private static int MAX_VALUE_RATING = 5;
    private static SparkConf conf;
    private static JavaSparkContext javaSparkContext;
    private static SparkSession sparkSession;
    private static String FILE_TYPE = ".log";
    private static String collectionRecommendations = "userRecommendations";
    private static String collectionStoreName = "userStoreRecommendations";
    private static String collectionOrganizationName = "userOrganizationRecommendations";
    private static String databaseName = "recommendationDB";
    private static String homeFolder = "C:/Users/Jonna/OneDrive/Skrivbord/";
    //private static String homeFolder = "C:/Users/Anton/Desktop/";
    //private static String homeFolder = "/home/jonna/Desktop/";
    private static String rootLogFolder = "/Logs/v1";


    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.FATAL);

        if (args[0].equals("--concat")) {
            String year = args[1].substring(0, 2);
            String month = args[1].substring(2, 4);
            String inputFolder = homeFolder + rootLogFolder + "/20" + year + "/" + month + "/*";
            String outputFolder = homeFolder + "/" + args[1] + FILE_TYPE;
            concatFiles(inputFolder, outputFolder);
        } else if (args[0].equals("--build")) {
            build(args[1]);

        } else if (args[0].equals("--parseDefault")) {
            parseCreateRatings(args[1]);
        } else if (args[0].equals("--parse")) {
            parseCreateRatings(args[1]);
        } else if (args[0].equals("--model")) {
            createModels(args[1]);
        } else if (args[0].equals("--test")) {
            test(homeFolder + args[1] + FILE_TYPE);
        }


    }

    private static void test(String userEvents) {
        conf = new SparkConf()
                .setMaster("local[100]")
                .setAppName("Parsing");

        javaSparkContext = new JavaSparkContext(conf);

        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();


        JavaRDD<Transaction> transactions = javaSparkContext.textFile(userEvents)
                .map(Transaction::parseLine);
/*
        // Get a list of organizations
        List<Integer> organizationIdList = transactions
                .map(t -> t.getOrganizationId())
                .distinct()
                .collect();

        for (Integer organizationId : organizationIdList) {
            // Create recommendations based on organizationIds
            // createModelOrganization(transactions, organizationId, false);

            JavaRDD<Integer> storeIds = transactions
                    .filter(t -> t.getOrganizationId() == organizationId)
                    .map(t -> t.getStoreId())
                    .distinct();

            // Create recommendations based on the organizationId and storeId
            storeIds.foreach(storeId -> System.out.println(organizationId + ", " + storeId));

            JavaRDD<String> timeOfDays = transactions
                    .filter(t -> t.getOrganizationId() == organizationId)
                    .map(t -> t.getEventDateBlock().getTimeOfDay())
                    .distinct();

            timeOfDays.foreach(timeOfDay -> System.out.println(timeOfDay));
        }
*/
        modelTest(transactions);


    }

    private static void modelTest(JavaRDD<Transaction> transactions) {


        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = sparkSession.createDataFrame(transactions, Transaction.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = sparkSession.sql("SELECT eventId FROM people");

        teenagersDF.show(1);

    }

    // Takes an input folder, reads all the events and concatenate them into a single file (monthly events)
    private static void concatFiles(String inputFolder, String outputFolder) throws IOException {
        conf = new SparkConf().setMaster("local[100]")
                .setAppName("Parsing");
        javaSparkContext = new JavaSparkContext(conf);

        javaSparkContext.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");
        long start = currentTimeMillis();
        List<String> logJson = javaSparkContext.textFile(inputFolder)
                .collect();

        FileWriter writer = new FileWriter(outputFolder);
        for (String line : logJson) {
            writer.write(line + "\n");
        }

        System.out.println("User events written to file.");
        writer.close();
    }

    /*
     * Reads the events given a certain input file
     * Create ratings
     * Builds the model and recommendations
     * Inserts into mongoDB
     */
    private static void build(String inputFile) {
        String transactionRatingFileName = parseCreateRatings(inputFile);
        createModels(transactionRatingFileName);
    }

    // Reads events from a file and create ratings from 1-5
    private static String parseCreateRatings(String inputArg) {
        String inputFile = homeFolder + inputArg + FILE_TYPE;
        String outputFile = homeFolder + inputArg + "ratings" + FILE_TYPE;

        System.out.println("Parsing JSON data from file...");

        conf = new SparkConf()
                .setMaster("local[100]")
                .setAppName("Parsing");

        javaSparkContext = new JavaSparkContext(conf);

        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<Transaction> transactions = javaSparkContext.textFile(inputFile)
                .map(Transaction::parseLine)
                .filter(t -> t.getBaseConsumer() != null && t.getProductId() != 0);


        // Get all transactions made by each consumer
        JavaPairRDD<Integer, Iterable<Transaction>> transactionsConsumers = transactions
                .groupBy(t -> t.getBaseConsumer().getConsumerId());

        // Get all transactions made for each product
        JavaPairRDD<Long, Iterable<Transaction>> transactionsProducts = transactions
                .groupBy(t -> t.getProductId());

        Map<Integer, Iterable<Transaction>> consumerTransactions = transactionsConsumers.collectAsMap();
        Map<Long, Iterable<Transaction>> productTransactions = transactionsProducts.collectAsMap();
        Map<Long, List<Double>> APproduct = new HashedMap();

        transactions.foreach(transaction -> {
            int rating;
            // Check if a product has been deleted, if so, the rating is set to 1 (minimum rating)
            if (transaction.getOrderElementChangeType() != null && transaction.getOrderElementChangeType().equals("DELETION")) {
                rating = MIN_VALUE_RATING;
            } else {
                int currentConsumer = transaction.getBaseConsumer().getConsumerId();

                // Get total amount of product bought by the current customer
                int totalBoughtProducts = ((Collection<?>) consumerTransactions.get(transaction.getBaseConsumer().getConsumerId())).size();
                int boughtProduct = 0;

                // Get the consumer transactions for the current product
                for (Transaction boughtProductTransaction : productTransactions.get(transaction.getProductId())) {
                    // Check if the product has been bought by the current consumer
                    if (currentConsumer == boughtProductTransaction.getBaseConsumer().getConsumerId()) {
                        boughtProduct++;
                    }
                }

                double AP = Math.log(((double) boughtProduct / (double) totalBoughtProducts) + 1);
                double maxAP = 0;

                // Check if AP for the product exists
                if (APproduct.containsKey(transaction.getProductId())) {
                    APproduct.get(transaction.getProductId()).add(AP);
                    maxAP = Collections.max(APproduct.get(transaction.getProductId()));
                } else {
                    ArrayList<Double> APs = new ArrayList<>();
                    APs.add(AP);
                    APproduct.put(transaction.getProductId(), APs);
                    maxAP = AP;
                }


                float RP = (float) AP / (float) maxAP;
                rating = (int) Math.ceil(5 * RP);
            }

            transaction.getBaseConsumer().setRating(rating);

            writeToFile(outputFile, transaction);
        });


        System.out.println("Done parsing.");
        return outputFile;
    }

    // Create recommendations based on all transactions
    private static void createModels(String inputEvents) {
        String eventsRating = homeFolder + inputEvents + FILE_TYPE;

        conf = new SparkConf()
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionStoreName)
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionOrganizationName)
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionRecommendations)
                .setMaster("local[*]")
                .setAppName("Model");

        javaSparkContext = new JavaSparkContext(conf);

        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        // Read the events with ratings
        Dataset<Row> events = sparkSession.read().json(eventsRating);

        events.createOrReplaceTempView("events");

        Dataset<Row> organizationIds = sparkSession.sql("SELECT DISTINCT organizationId FROM events");

        System.out.println("Building recommendations based on organizations:");
        organizationIds.foreach(r -> {
            long organizationId = (long) r.get(0);
            Dataset<Row> transactionsOrganization = sparkSession.sql("SELECT baseConsumer.consumerId, baseConsumer.rating, productId FROM events WHERE organizationId = " + organizationId);
            createModelOrganization(transactionsOrganization, organizationId);
        });

        /*

        long start = currentTimeMillis();
        Dataset<Row> transactions = sparkSession.sql("SELECT baseConsumer.consumerId, baseConsumer.rating, productId FROM events");

        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactions.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactions.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("consumerId")
                .setRatingCol("rating")
                .setItemCol("productId");


        // Train a model from the dataset
        ALSModel model = als.fit(trainingDs);

        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);


        System.out.println("num of users: " + consumerRecommends.count());

        long end = currentTimeMillis();

        long elapsedTime = end - start;

        System.out.println("Took " + elapsedTime);

        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();

*/

    }

    private static void createModelOrganization(Dataset<Row> transactionsOrganization, long organizationId) {
        long start = currentTimeMillis();

        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactionsOrganization.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactionsOrganization.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("consumerId")
                .setRatingCol("rating")
                .setItemCol("productId");


        // Train a model from the dataset (this could be using whole dataset as well if needed)
        ALSModel model = als.fit(trainingDs);

        // Future work, to measure accuracy for the model
        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);


        System.out.println("num of users: " + consumerRecommends.count());

        long end = currentTimeMillis();

        long elapsedTime = end - start;

        System.out.println("[createModelOrganization] - elapsed time " + elapsedTime);

        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();
        recommendations.foreach(document -> {
            // Maybee the cast is not needed (mongodb stores the value as NumberLong(1))
            document.append("organizationId", (int) organizationId);
        });

        writeToDatabase(recommendations, collectionOrganizationName);
    }

    private static void createModelStore(Dataset<Row> transactionsOrganization, int organizationId) {
        long start = currentTimeMillis();

        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactionsOrganization.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactionsOrganization.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("consumerId")
                .setRatingCol("rating")
                .setItemCol("productId");


        // Train a model from the dataset
        ALSModel model = als.fit(trainingDs);

        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);


        System.out.println("num of users: " + consumerRecommends.count());

        long end = currentTimeMillis();

        long elapsedTime = end - start;

        System.out.println("[createModelOrganization] - elapsed time " + elapsedTime);

        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();
        recommendations.foreach(document -> {
            document.append("organizationId", organizationId);
        });

        writeToDatabase(recommendations, collectionOrganizationName);
    }


    private static void writeToDatabase(JavaRDD<Document> recommendations, String collectionName) {
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("database", databaseName);
        writeOverrides.put("collection", collectionName);
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(javaSparkContext).withOptions(writeOverrides);

        MongoSpark.save(recommendations, writeConfig);
        System.out.println("Saved recommendations in database");
    }





    private static void writeToFile(String outputFile, Transaction transaction) throws IOException {
        BufferedWriter writer = new BufferedWriter
                (new OutputStreamWriter(new FileOutputStream(outputFile, true), StandardCharsets.ISO_8859_1));
        ObjectMapper mapper = new ObjectMapper();
        writer.append(mapper.writeValueAsString(transaction) + "\n");
        writer.close();
    }

}