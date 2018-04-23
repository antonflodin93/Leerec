package POJO;

import java.io.Serializable;

public class BaseConsumer implements Serializable {
    private int consumerId;
    private String consumerPhase;
    private String consumerLoyaltyState;
    private String consumerLoyaltyChangeState;
    private int rating;

    public BaseConsumer() {
    }

    public BaseConsumer(int consumerId, String consumerPhase, String consumerLoyaltyState, String consumerLoyaltyChangeState, int rating) {
        this.consumerId = consumerId;
        this.consumerPhase = consumerPhase;
        this.consumerLoyaltyState = consumerLoyaltyState;
        this.consumerLoyaltyChangeState = consumerLoyaltyChangeState;
        this.rating = rating;
    }

    public BaseConsumer(int consumerId, int rating) {
        this.consumerId = consumerId;
        this.rating = rating;
    }

    public BaseConsumer(BaseConsumer baseConsumer) {
    }

    public int getConsumerId() {
        return consumerId;
    }

    public void setConsumerId(int consumerId) {
        this.consumerId = consumerId;
    }

    public String getConsumerPhase() {
        return consumerPhase;
    }

    public void setConsumerPhase(String consumerPhase) {
        this.consumerPhase = consumerPhase;
    }

    public String getConsumerLoyaltyState() {
        return consumerLoyaltyState;
    }

    public void setConsumerLoyaltyState(String consumerLoyaltyState) {
        this.consumerLoyaltyState = consumerLoyaltyState;
    }

    public String getConsumerLoyaltyChangeState() {
        return consumerLoyaltyChangeState;
    }

    public void setConsumerLoyaltyChangeState(String consumerLoyaltyChangeState) {
        this.consumerLoyaltyChangeState = consumerLoyaltyChangeState;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

}
