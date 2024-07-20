package com.example.nilera.data;

import com.example.nilera.data.producer.DataProducer;

public class ScenicApp {
    public static final String url = "http://192.168.26.111:9527/logweb/upload";
    public static void main(String[] args) throws Exception{
        DataProducer.producer(url);
    }
}
