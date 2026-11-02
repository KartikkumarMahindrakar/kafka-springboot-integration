package io.github.kartikkumarmahindrakar.kafka.streams.spring.integration.controller;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SentenceStoreController {

    private final StreamsBuilderFactoryBean factoryBean;

    public SentenceStoreController(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    @GetMapping("/sentences/{key}")
    public String getSentence(@PathVariable String key){
        KafkaStreams streams = factoryBean.getKafkaStreams();
        var params = StoreQueryParameters.fromNameAndType("SentencesStore", QueryableStoreTypes.<String, String>keyValueStore());
        ReadOnlyKeyValueStore<String, String> sentenceStore = streams.store(params);
        return sentenceStore.get(key);
    }
}
