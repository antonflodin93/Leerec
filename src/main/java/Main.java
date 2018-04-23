import POJO.Recommendation;
import POJO.Transaction;
import com.mongodb.spark.MongoSpark;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static java.lang.System.currentTimeMillis;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Main {
    private static int MIN_VALUE_RATING = 1;
    private static int MAX_VALUE_RATING = 5;
    private static SparkConf conf;
    private static JavaSparkContext javaSparkContext;
    private static SparkSession sparkSession;
    private static String FILE_TYPE = ".log";
    private static String collectionStoreName = "userStoreRecommendations";
    private static String databaseName = "recommendationDB";
    //private static String homeFolder = "C:/Users/Jonna/OneDrive/Skrivbord/";
    private static String homeFolder = "C:/Users/Anton/Desktop/";
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
            parseCreateRatings(args[1], true, true);
        } else if (args[0].equals("--parse")) {
            parseCreateRatings(args[1], false, true);
        } else if (args[0].equals("--model")) {
            createModel(args[1], true);
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

    private static void modelTest(JavaRDD<Transaction> transactions){


        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = sparkSession.createDataFrame(transactions, Transaction.class);
        // Register the DataFrame as a temporary view
        peopleDF.createOrReplaceTempView("people");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = sparkSession.sql("SELECT eventId FROM people");

        teenagersDF.show(1);

    }

    private static void build(String inputFile) {
        System.out.println("Parsing...");
        JavaRDD<Transaction> transactionRatings = parseCreateRatings(inputFile, false, false);
        System.out.println("Building model...");
        createModel(inputFile, false);

    }

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

    private static JavaRDD<Transaction> parseCreateRatings(String inputArg, boolean defaultRating, boolean testing) {
        String inputFile = homeFolder + inputArg + FILE_TYPE;
        String outputFile;
        if (defaultRating) {
            outputFile = homeFolder + inputArg + "ratingsDefault" + FILE_TYPE;
        } else {
            outputFile = homeFolder + inputArg + "ratings" + FILE_TYPE;
        }


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


        // ObjectMapper mapper = new ObjectMapper();

        if (!defaultRating) {
            JavaPairRDD<Integer, Iterable<Transaction>> transactionsConsumers = transactions
                    .groupBy(t -> t.getBaseConsumer().getConsumerId());

            JavaPairRDD<Long, Iterable<Transaction>> transactionsProducts = transactions
                    .groupBy(t -> t.getProductId());

            Map<Integer, Iterable<Transaction>> consumerTransactions = transactionsConsumers.collectAsMap();
            Map<Long, Iterable<Transaction>> productTransactions = transactionsProducts.collectAsMap();
            Map<Long, List<Double>> APproduct = new HashedMap();

            transactions.foreach(transaction -> {
                int rating;
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

                if (testing) {
                    //BufferedWriter writer = new BufferedWriter
                    //       (new OutputStreamWriter(new FileOutputStream(outputFile, true), StandardCharsets.ISO_8859_1));
                    //writer.append(mapper.writeValueAsString(transaction) + "\n");
                    //writer.close();
                }
            });


        } else {
            transactions.foreach(transaction -> {
                int rating;
                if (transaction.getOrderElementChangeType().equals("DELETION")) {
                    rating = MIN_VALUE_RATING;
                } else {
                    rating = MAX_VALUE_RATING;

                    transaction.getBaseConsumer().setRating(rating);

                    if (testing) {
                        //BufferedWriter writer = new BufferedWriter
                        //       (new OutputStreamWriter(new FileOutputStream(outputFile, true), StandardCharsets.ISO_8859_1));
                        //writer.append(mapper.writeValueAsString(transaction) + "\n");
                        //writer.close();
                    }
                }
            });
        }

        System.out.println("Done parsing.");
        System.out.println(transactions.count());
        return transactions;
    }

    private static void createModel(String inputEvents, boolean testing) {
        String modelFolder = homeFolder + inputEvents + "model";
        String eventsRating = homeFolder + inputEvents + FILE_TYPE;

        conf = new SparkConf()
                .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionStoreName)
                .setMaster("local[100]")
                .setAppName("Model");

        javaSparkContext = new JavaSparkContext(conf);

        sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> events = sparkSession.read().json(eventsRating);

        events.printSchema();

        events.createOrReplaceTempView("events");


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


        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();

        recommendations.foreach(document -> {
            document.append("storeId", 1);
        });


        MongoSpark.save(recommendations);
        System.out.println("Saved recommendations in database");

    }

    private static void createModelOrganization(JavaRDD<Transaction> allTransactions, int organizationId, boolean testing) {
        conf = new SparkConf()
                .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/" + databaseName + "." + collectionStoreName)
                .setMaster("local[100]")
                .setAppName("Model");


/*

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("baseConsumer", DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("consumerId", DataTypes.LongType, true),
                        DataTypes.createStructField("consumerLoyaltyChangeState", DataTypes.StringType, true),
                        DataTypes.createStructField("consumerLoyaltyState", DataTypes.StringType, true),
                        DataTypes.createStructField("consumerPhase", DataTypes.StringType, true),
                        DataTypes.createStructField("rating", DataTypes.LongType, true)
                )), true),
                DataTypes.createStructField("campaignId", DataTypes.LongType, true),
                DataTypes.createStructField("currency", DataTypes.StringType, true),
                DataTypes.createStructField("discount", DataTypes.LongType, true),
                DataTypes.createStructField("eventDateBlock", DataTypes.createStructType(Arrays.asList(
                        DataTypes.createStructField("epochMilli", DataTypes.LongType, true),
                        DataTypes.createStructField("hour", DataTypes.LongType, true),
                        DataTypes.createStructField("localDateTime", DataTypes.LongType, true),
                        DataTypes.createStructField("month", DataTypes.StringType, true),
                        DataTypes.createStructField("quarter", DataTypes.LongType, true),
                        DataTypes.createStructField("timeOfDay", DataTypes.StringType, true),
                        DataTypes.createStructField("week", DataTypes.LongType, true),
                        DataTypes.createStructField("weekDay", DataTypes.LongType, true),
                        DataTypes.createStructField("year", DataTypes.LongType, true)
                )), true),
                DataTypes.createStructField("eventId", DataTypes.StringType, true),
                DataTypes.createStructField("eventType", DataTypes.StringType, true),
                DataTypes.createStructField("externalId", DataTypes.StringType, true),
                DataTypes.createStructField("orderElementChangeType", DataTypes.StringType, true),
                DataTypes.createStructField("orderElementName", DataTypes.StringType, true),
                DataTypes.createStructField("orderElementType", DataTypes.StringType, true),
                DataTypes.createStructField("orderExternalId", DataTypes.StringType, true),
                DataTypes.createStructField("orderId", DataTypes.LongType, true),
                DataTypes.createStructField("organizationId", DataTypes.LongType, true),
                DataTypes.createStructField("organizationName", DataTypes.StringType, true),
                DataTypes.createStructField("paymentType", DataTypes.StringType, true),
                DataTypes.createStructField("price", DataTypes.LongType, true),
                DataTypes.createStructField("productId", DataTypes.LongType, true),
                DataTypes.createStructField("productOptionGroupId", DataTypes.LongType, true),
                DataTypes.createStructField("storeId", DataTypes.LongType, true),
                DataTypes.createStructField("storeLocationCity", DataTypes.StringType, true),
                DataTypes.createStructField("storeName", DataTypes.StringType, true)
        ));

*/


        Dataset<Row> events = sparkSession.createDataFrame(allTransactions, Transaction.class).toDF();



        events.createOrReplaceTempView("events");

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


        JavaRDD<Document> recommendations = consumerRecommends.toJSON()
                .toJavaRDD()
                .map(Transaction::convertToDocument).cache();

        recommendations.foreach(document -> {
            document.append("storeId", 1);
        });


        MongoSpark.save(recommendations);
        System.out.println("Saved recommendations in database");

    }

    private static void createModelStore(JavaRDD<Transaction> transactions, int storeId, boolean testing) {

    }

    private static void createModelTimeOfDay(String inputEvents, String timeOfDay, boolean testing) {

    }

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}