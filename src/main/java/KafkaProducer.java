import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import config.Kafka;
import config.Twitter;
import model.Tweet;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KafkaProducer {

    private final BlockingQueue<String> queue;
    private final Client client;
    private final Gson gson;

    public KafkaProducer() {
        // Configure auth
        Authentication authentication = new OAuth1(
                Twitter.CONSUMER_KEY,
                Twitter.CONSUMER_SECRET,
                Twitter.ACCESS_TOKEN,
                Twitter.TOKEN_SECRET);

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        List<Location> locations = new ArrayList<>();
        locations.add(Twitter.SF_LOCATION);
        endpoint.locations(locations);


        queue = new LinkedBlockingQueue<>(10000);

        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        gson = new Gson();

    }

    private Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafka.SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafkaproducer_twitter");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
    }

    private void run() {
        client.connect();
        try (Producer<Long, String> producer = getProducer()) {
            //noinspection InfiniteLoopStatement
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                long key = tweet.getId();
                String msg = tweet.getText();
                System.out.println("Fetched tweet " + msg);
                ProducerRecord<Long, String> record = new ProducerRecord<>(Kafka.Topic.SAN_FRANCISCO.name(), key, msg);
                producer.send(record);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }

    public static void main(String[] args) {
        new KafkaProducer().run();
    }
}