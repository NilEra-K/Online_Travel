package com.example.nilera.travel.travelweb.services;

import com.example.nilera.travel.travelweb.entity.Scenic;

import java.util.HashMap;
import java.util.List;

// 接口: 定义规范的
public interface TravelService {
    long getDaySum();
    long getCurrentSum();
    List<Scenic> getHeatData();
    List<Scenic> getScenicMinuteData();
    HashMap<String, Object> getScenicTread();

    List<Scenic> getHeatDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    List<Scenic> getScenicDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    HashMap<String, Object> getScenicTreadByDateAndTime(String fromDateAndTime, String toDateAndTime);
}
