package POJO;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import org.bson.Document;

import java.io.Serializable;
import java.io.StringReader;

public class Transaction implements Serializable {
    private String eventId;
    private String eventType;
    private String paymentType;
    private int orderId;
    private String orderExternalId;
    private BaseConsumer baseConsumer;
    //private long consumerId;
    private EventDateBlock eventDateBlock;
    private int organizationId;
    private String organizationName;
    private int storeId;
    private String storeName;
    private String storeLocationCity;
    private int campaignId;
    private String orderElementType;
    private String orderElementChangeType;
    private String orderElementName;
    private long productId;
    private long productOptionGroupId;
    private int price;
    private int discount;
    private String currency;


    public Transaction() {
        super();
    }

    public static Transaction parseLine(String line) {

        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new StringReader(line));
        reader.setLenient(true);
        Transaction transaction = gson.fromJson(reader, Transaction.class);


        return transaction;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public String getOrderExternalId() {
        return orderExternalId;
    }

    public void setOrderExternalId(String orderExternalId) {
        this.orderExternalId = orderExternalId;
    }

    public BaseConsumer getBaseConsumer() {
        return baseConsumer;
    }

    public void setBaseConsumer(BaseConsumer baseConsumer) {
        this.baseConsumer = baseConsumer;
    }

    public EventDateBlock getEventDateBlock() {
        return eventDateBlock;
    }

    public void setEventDateBlock(EventDateBlock eventDateBlock) {
        this.eventDateBlock = eventDateBlock;
    }

    public int getOrganizationId() {
        return organizationId;
    }

    public void setOrganizationId(int organizationId) {
        this.organizationId = organizationId;
    }

    public String getOrganizationName() {
        return organizationName;
    }

    public void setOrganizationName(String organizationName) {
        this.organizationName = organizationName;
    }

    public int getStoreId() {
        return storeId;
    }

    public void setStoreId(int storeId) {
        this.storeId = storeId;
    }

    public String getStoreName() {
        return storeName;
    }

    public void setStoreName(String storeName) {
        this.storeName = storeName;
    }

    public String getStoreLocationCity() {
        return storeLocationCity;
    }

    public void setStoreLocationCity(String storeLocationCity) {
        this.storeLocationCity = storeLocationCity;
    }

    public int getCampaignId() {
        return campaignId;
    }

    public void setCampaignId(int campaignId) {
        this.campaignId = campaignId;
    }

    public String getOrderElementType() {
        return orderElementType;
    }

    public void setOrderElementType(String orderElementType) {
        this.orderElementType = orderElementType;
    }

    public String getOrderElementChangeType() {
        return orderElementChangeType;
    }

    public void setOrderElementChangeType(String orderElementChangeType) {
        this.orderElementChangeType = orderElementChangeType;
    }

    public String getOrderElementName() {
        return orderElementName;
    }

    public void setOrderElementName(String orderElementName) {
        this.orderElementName = orderElementName;
    }

    public long getProductId() {
        return productId;
    }

    public void setProductId(long productId) {
        this.productId = productId;
    }

    public long getProductOptionGroupId() {
        return productOptionGroupId;
    }

    public void setProductOptionGroupId(long productOptionGroupId) {
        this.productOptionGroupId = productOptionGroupId;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getDiscount() {
        return discount;
    }

    public void setDiscount(int discount) {
        this.discount = discount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public static String getJsonFormat(Transaction transaction){
        Gson gson = new GsonBuilder().create();
        return gson.toJson(transaction);

    }

    public static Document convertToDocument(String transaction){
        return Document.parse(transaction);
    }

}
