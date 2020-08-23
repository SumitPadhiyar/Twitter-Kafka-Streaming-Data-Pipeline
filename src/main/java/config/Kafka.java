package config;

public class Kafka {
    public static final String SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    public static final long SLEEP_TIMER = 1000;

    public enum Topic{
        SAN_FRANCISCO;
    }
}
