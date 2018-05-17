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
import java.sql.Timestamp;
import java.util.*;

import static java.lang.System.currentTimeMillis;

public class Main {
    private static int MIN_VALUE_RATING = 1;
    private static int MAX_VALUE_RATING = 5;
    private static SparkConf conf;
    private static JavaSparkContext javaSparkContext;
    private static SparkSession sparkSession;
    private static String FILE_TYPE = ".log";
    private static String collectionGeneralRecommendations = "userGeneralRecommendations";
    private static String collectionWeekDayName = "userWeekDayRecommendations";
    private static String collectionTimeOfDayName = "userTimeOfDayRecommendations";
    private static String collectionOrganizationName = "userOrganizationRecommendations";
    private static String databaseName = "recommendationDB";
    private static int ALS_MAX_ITERATIONS = 5;
    private static double ALS_REG_PARAMETER = 0.01;
    private static String ALS_USER_COLUMN = "consumerId";
    private static String ALS_RATING_COLUMN = "rating";
    private static String ALS_ITEM_COLUMN = "productId";

    public enum RecommendationType {
        GENERAL,
        ORGANIZATION,
        WEEKDAY,
        TIMEOFDAY
    }


    private static String rootFolder;

    private static String rootLogFolder = "/Logs/v1";


    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.FATAL);


        if (args[0].equals("-concat") && args.length == 3) {
            String year = args[1].substring(0, 2);
            String month = args[1].substring(2, 4);
            rootFolder = args[2];
            String inputFolder = rootFolder + rootLogFolder + "/20" + year + "/" + month + "/*";
            String outputFolder = rootFolder + "/" + args[1] + FILE_TYPE;
            concatFiles(inputFolder, outputFolder);
        } else if (args[0].equals("-parse") && args.length == 3) {
            rootFolder = args[2];
            parseCreateRatings(args[1]);
        } else if (args[0].equals("-model") && args.length == 3) {
            rootFolder = args[2];
            createModels(args[1]);
        } else if (args[0].equals("-build") && args.length == 3) {
            String year = args[1].substring(0, 2);
            String month = args[1].substring(2, 4);
            rootFolder = args[2];
            String inputFolder = rootFolder + rootLogFolder + "/20" + year + "/" + month + "/*";
            String outputFolder = rootFolder + "/" + args[1] + FILE_TYPE;
            build(inputFolder, outputFolder);
        } else{
            System.out.println("Usage: ");
            System.out.println("jarfile [--operation] [source folder/file] [rootfolder] where operation can be the following:\n");
            System.out.println("-concat [source folder] [root folder] - Reads all log files from the given source folder (monthly) and outputs single file with the events. \n" +
                    "Example: -concat \"1801\" \"C:/User/Desktop\", which concatenates all log files from January 2018.\n");
            System.out.println("-parse [source file] [root folder] - Reads the events in the given source file, parse the " +
                    "events and create a rating for each transaction, outputs to a file with the transactions, \n" +
                    "Example: -parse \"1801\" \"C:/User/Desktop\", which create ratings for all transactions in January 2018.\n");
            System.out.println("-model [source file] [root folder] - Create product recommendations based on the transactions in the source file, inserts the recommendations in MongoDB, \n" +
                    "Example: -model \"1801ratings\" \"C:/User/Desktop\", which create recommendations for the transactions made in January 2018.\n");
            System.out.println("-build [source folder] [root folder] - Performs all the steps: Concatenates, parsing and model, " +
                    "Example: -build \"1801\" \"User/Desktop\"");
        }
    }

    /*
     * Reads the events given a certain input file
     * Create ratings
     * Builds the models and recommendations
     * Inserts into mongoDB
     */
    private static void build(String eventFolder, String outputFile) throws IOException {
        concatFiles(eventFolder, outputFile);
        String transactionRatingFileName = parseCreateRatings(outputFile);
        createModels(transactionRatingFileName);
    }

    // Takes an input folder, reads all the events and concatenate them into a single file (monthly events)
    private static void concatFiles(String inputFolder, String outputFolder) throws IOException {
        System.out.println("Concatenating files...");
        conf = new SparkConf().setMaster("local[*]")
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
        writer.close();

        long end = currentTimeMillis();
        System.out.println("User events written to file.");
        System.out.println("Took: " + (end - start));
    }

    // Reads events from a file and create ratings from 1-5
    private static String parseCreateRatings(String inputArg) {
        String inputFile = rootFolder + inputArg + FILE_TYPE;
        String outputFile = rootFolder + inputArg + "ratings" + FILE_TYPE;

        System.out.println("Parsing JSON data from file...");

        conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Parsing");

        javaSparkContext = new JavaSparkContext(conf);

        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        long start = currentTimeMillis();

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
                rating = (int) Math.ceil(MAX_VALUE_RATING * RP);
            }

            transaction.getBaseConsumer().setRating(rating);

            writeToFile(outputFile, transaction);

        });

        long end = currentTimeMillis();

        System.out.println("Took: " + (end - start));
        System.out.println("Done parsing.");
        return outputFile;
    }

    // Create recommendations based on all transactions
    private static void createModels(String inputEvents) {
        System.out.println("Creating models...");
        String eventsRating = rootFolder + inputEvents + FILE_TYPE;

        conf = new SparkConf()
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionGeneralRecommendations)
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionWeekDayName)
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionTimeOfDayName)
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionOrganizationName)
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

        long startWeekDay = currentTimeMillis();
        Dataset<Row> weekDays = sparkSession.sql("SELECT eventDateBlock.weekDay FROM events").distinct();

        System.out.println("Building recommendations based on weekDay");
        weekDays.foreach(r -> {
            long weekDay = (long) r.get(0);
            Dataset<Row> transactionStores = sparkSession.sql("SELECT baseConsumer.consumerId, baseConsumer.rating, productId FROM events WHERE eventDateBlock.weekDay = " + weekDay);
            createModelWeekDay(transactionStores, weekDay);
        });
        long endWeekDay = currentTimeMillis();


        long startTimeOfDay = currentTimeMillis();
        Dataset<Row> timeOfDays = sparkSession.sql("SELECT eventDateBlock.timeOfDay FROM events").distinct();

        System.out.println("Building recommendations based on timeofday");
        timeOfDays.foreach(r -> {
            String timeOfDay = (String) r.get(0);
            Dataset<Row> transactionStores = sparkSession.sql("SELECT baseConsumer.consumerId, baseConsumer.rating, productId FROM events WHERE eventDateBlock.timeOfDay = \"" + timeOfDay + "\"");
            createModelTimeOfDay(transactionStores, timeOfDay);
        });
        long endTimeOfDay = currentTimeMillis();


        long startOrg = currentTimeMillis();

        System.out.println("Building recommendations based on organizations");
        Dataset<Row> organizationIds = sparkSession.sql("SELECT DISTINCT organizationId FROM events");

        organizationIds.foreach(r -> {
            long organizationId = (long) r.get(0);
            Dataset<Row> transactionsOrganization = sparkSession.sql("SELECT baseConsumer.consumerId, baseConsumer.rating, productId FROM events WHERE organizationId = " + organizationId);
            createModelOrganization(transactionsOrganization, organizationId);
        });
        long endOrg = currentTimeMillis();

        long allStart = currentTimeMillis();
        Dataset<Row> transactions = sparkSession.sql("SELECT baseConsumer.consumerId, baseConsumer.rating, productId FROM events");

        // Future work (to get accuracy)
        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactions.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactions.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(ALS_MAX_ITERATIONS)
                .setRegParam(ALS_REG_PARAMETER)
                .setUserCol(ALS_USER_COLUMN)
                .setRatingCol(ALS_RATING_COLUMN)
                .setItemCol(ALS_ITEM_COLUMN);


        ALSModel model = als.fit(transactions);

        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);

        long allEnd = currentTimeMillis();


        long allElapsedTime = allEnd - allStart;
        long timeOfDayElapsedTime = endTimeOfDay - startTimeOfDay;
        long orgElapsedTime = endOrg - startOrg;
        long WeakDayElapsedTime = endWeekDay - startWeekDay;


        System.out.println("[All transactions]: " + allElapsedTime + ", " + consumerRecommends.count());
        System.out.println("[Organization transactions]: " + orgElapsedTime + ", " + organizationIds.count());
        System.out.println("[TimeOfDay transactions]: " + timeOfDayElapsedTime + ", " + timeOfDays.count());
        System.out.println("[WeekDay transactions]: " + WeakDayElapsedTime + ", " + weekDays.count());

        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();


        recommendations.foreach(document -> {
            document.append("timestamp", new Timestamp(System.currentTimeMillis()).toString());
        });

        writeToDatabase(recommendations, collectionGeneralRecommendations);


    }


    private static void createModelOrganization(Dataset<Row> transactionsOrganization, long organizationId) {
        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactionsOrganization.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactionsOrganization.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(ALS_MAX_ITERATIONS)
                .setRegParam(ALS_REG_PARAMETER)
                .setUserCol(ALS_USER_COLUMN)
                .setRatingCol(ALS_RATING_COLUMN)
                .setItemCol(ALS_ITEM_COLUMN);


        // Train a model from the dataset (this could be using whole dataset as well if needed)
        ALSModel model = als.fit(transactionsOrganization);

        // Future work, to measure accuracy for the model
        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);


        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();
        recommendations.foreach(document -> {
            // Maybee the cast is not needed (mongodb stores the value as NumberLong(1))
            document.append("organizationId", (int) organizationId);
            document.append("timestamp", new Timestamp(System.currentTimeMillis()).toString());
        });

        writeToDatabase(recommendations, collectionOrganizationName);
    }

    private static void createModelWeekDay(Dataset<Row> transactionsStores, long weekDay) {
        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactionsStores.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactionsStores.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(ALS_MAX_ITERATIONS)
                .setRegParam(ALS_REG_PARAMETER)
                .setUserCol(ALS_USER_COLUMN)
                .setRatingCol(ALS_RATING_COLUMN)
                .setItemCol(ALS_ITEM_COLUMN);


        // Train a model from the dataset (this could be using whole dataset as well if needed)
        ALSModel model = als.fit(transactionsStores);


        // Future work, to measure accuracy for the model
        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);

        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();
        recommendations.foreach(document -> {
            document.append("weekDay", weekDay);
            document.append("timestamp", new Timestamp(System.currentTimeMillis()).toString());
        });

        writeToDatabase(recommendations, collectionWeekDayName);
    }

    private static void createModelTimeOfDay(Dataset<Row> transactionsStores, String timeOfDay) {
        double[] splitPercantage = {0.8, 0.2};
        Dataset<Row> trainingDs = transactionsStores.randomSplit(splitPercantage)[0];
        Dataset<Row> testDs = transactionsStores.randomSplit(splitPercantage)[1];


        ALS als = new ALS()
                .setMaxIter(ALS_MAX_ITERATIONS)
                .setRegParam(ALS_REG_PARAMETER)
                .setUserCol(ALS_USER_COLUMN)
                .setRatingCol(ALS_RATING_COLUMN)
                .setItemCol(ALS_ITEM_COLUMN);


        // Train a model from the dataset (this could be using whole dataset as well if needed)
        ALSModel model = als.fit(transactionsStores);


        // Future work, to measure accuracy for the model
        Dataset<Row> predictions = model.transform(testDs);

        // 10 item recommendations for each consumer
        Dataset<Row> consumerRecommends = model.recommendForAllUsers(10);

        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();
        recommendations.foreach(document -> {
            document.append("timeOfDay", timeOfDay);
            document.append("timestamp", new Timestamp(System.currentTimeMillis()).toString());
        });

        writeToDatabase(recommendations, collectionTimeOfDayName);
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