package com.example.nilera.data.producer;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataProducer {
    public static void producer(String url) throws Exception{
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;
        //济南部分景点
        String[] sciences=new String[]{
                "大明湖",
                "趵突泉",
                "曲水亭街",
                "济南泉城广场",
                "千佛山",
                "芙蓉街",
                "济南国际园博园",
                "九如山风景区",
                "五龙潭公园",
                "济南方特东方神话",
                "解放阁",
                "洪家楼天主教堂",
                "黑虎泉",
                "七星台风景区",
                "济南龙门山景区",
                "济南石崮寨景区",
                "济南奥体中心",
                "济南融创文旅城",
                "济南龙洞风景区",
                "济南黄金谷山水画廊",
                "英雄山风景区",
                "济南卧虎山山体公园"
        };
        //景点对应的经纬度
        Map<String,String> map= new HashMap();
        map.put("大明湖", "117.023837,36.674997");
        map.put("趵突泉", "117.016089,36.661138");
        map.put("曲水亭街", "117.024489,36.669213");
        map.put("济南泉城广场", "117.021483,36.661473");
        map.put("千佛山", "117.034920,36.641749");
        map.put("芙蓉街", "117.022959,36.668068");
        map.put("济南国际园博园", "116.813744,36.541549");
        map.put("九如山风景区", "117.277569,36.485055");
        map.put("五龙潭公园", "117.014637,36.665992");
        map.put("济南方特东方神话", "116.878715,36.70584");
        map.put("解放阁", "117.033627,36.663178");
        map.put("洪家楼天主教堂", "117.065923,36.686232");
        map.put("黑虎泉", "117.033525,36.662414");
        map.put("七星台风景区", "117.314498,36.468696");
        map.put("济南龙门山景区", "117.192153,36.44956");
        map.put("济南石崮寨景区", "116.964764,36.521221");
        map.put("济南奥体中心", "117.120308,36.656973");
        map.put("济南融创文旅城", "117.196957,36.659616");
        map.put("济南龙洞风景区", "117.099376,36.601566");
        map.put("济南黄金谷山水画廊", "117.077066,36.621757");
        map.put("英雄山风景区", "117.004865,36.636092");
        map.put("济南卧虎山山体公园", "117.02227,36.614267");

        try{
            Random random=new Random();
            // int bound = 10000;
            // for(int i=0; i<random.nextInt(bound); i++){
            while(true) {
                String science = sciences[random.nextInt(sciences.length)];
                String longitudeAndlatitude = map.get(science);
                String time = FastDateFormat.getInstance("yyyyMMddHHmmss").format(new Date());
                String log = longitudeAndlatitude + ","+science+","+time;
                System.out.println(log);
                UploadUtil.upload(url, log);
                Thread.sleep(random.nextInt(50));
            }
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
