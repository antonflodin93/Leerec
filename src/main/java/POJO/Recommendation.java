package POJO;

import org.apache.spark.sql.Row;

public class Recommendation {
    private int consumerId;
    private long productId;
    private int storeId;
    private int organizationId;
    private String timeOfDay;
    private int weekDay;


    public static Recommendation parseRecommendation(Row row) {
        System.out.println(row);

        int consumerId = row.getAs("consumerId");

        return null;
    }
}
