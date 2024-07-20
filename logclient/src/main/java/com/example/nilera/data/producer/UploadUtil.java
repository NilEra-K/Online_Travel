package com.example.nilera.data.producer;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class UploadUtil {
    public static void upload(String path,String log){
        try {
            URL url = new URL(path);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type","application/json");
            OutputStream out = connection.getOutputStream();
            out.write(log.getBytes());
            out.flush();
            out.close();
            System.out.println(connection.getResponseCode());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
