package io.github.kartikkumarmahindrakar.kafka.streams.spring.integration.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SentencesTopology {

    @Autowired
    void process(StreamsBuilder streamsBuilder){
        KStream<String, String> stream = streamsBuilder.stream("input-topic", Consumed.with(Serdes.String(),Serdes.String()));
        stream.peek((k,v) -> System.out.printf("Incoming record - Key: %s, Value: %s%n",k,v))
                .groupByKey()
                .reduce((a,b)->a+" "+b, Materialized.as("SentencesStore"))
                .toStream()
                .peek((k,v)-> System.out.printf("Reduced - Key: %s, Value:%s%n",k,v));

    }
}
