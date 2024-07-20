package com.example.nilera.travel.travelweb.dao;

import com.example.nilera.travel.travelweb.entity.Scenic;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface TravelDao {
    long getDaySum();           // 查询当天时间内的所有数据之和
    long getCurrentSum();       // 查询当前时间内的所有数据之和
    List<Scenic> getHeatData();         // 查询用于创建热力图的数据
    List<Scenic> getScenicMinuteData(); // 查询用于获取每分钟景点人流量
    List<Scenic> getScenicTread();      // 查询用于获取每分钟景点人流量趋势

    List<Scenic> getHeatDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    List<Scenic> getScenicDataByDateAndTime(String fromDateAndTime, String toDateAndTime);
    List<Scenic> getScenicTreadByDateAndTime(String fromDateAndTime, String toDateAndTime);
}
