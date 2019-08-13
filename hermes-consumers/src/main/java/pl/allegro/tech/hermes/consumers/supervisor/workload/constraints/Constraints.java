package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Constraints {

    private final int consumersNumber;

    @JsonCreator
    public Constraints(@JsonProperty("consumersNumber") int consumersNumber) {
        this.consumersNumber = consumersNumber;
    }

    public int getConsumersNumber() {
        return consumersNumber;
    }
}
