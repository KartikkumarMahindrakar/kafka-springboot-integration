package io.github.kartikkumarmahindrakar.kafka.streams.spring.integration.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SentenceStoreController {
    @GetMapping("/sentences/{key}")
    public String getSentence(@PathVariable String key){
        return "hello world";
    }
}
