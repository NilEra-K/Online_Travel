package com.example.nilera.travel.travelweb.services.impl;

import com.example.nilera.travel.travelweb.dao.TravelDao;
import com.example.nilera.travel.travelweb.entity.LineData;
import com.example.nilera.travel.travelweb.entity.Scenic;
import com.example.nilera.travel.travelweb.services.TravelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Service
public class TravelServiceImpl implements TravelService {
    // 调用某个对象的方法, 需要先创建对象
    @Autowired
    private TravelDao travelDao;    // 相当于 TravelDap travelDao = new TravelDao();

    @Override
    public long getDaySum() {
        return travelDao.getDaySum() ;
    }

    @Override
    public long getCurrentSum() {
        return travelDao.getCurrentSum();
    }

    @Override
    public List<Scenic> getHeatData() {
        return travelDao.getHeatData();
    }

    @Override
    public List<Scenic> getScenicMinuteData() {
        return travelDao.getScenicMinuteData();
    }

    @Override
    public HashMap<String, Object> getScenicTread() {
        HashMap<String, Object> maps = new HashMap<>();
        List<String> times = new ArrayList<>();     // 时间数组
        List<LineData> series = new ArrayList<>();  // Series 数组

        List<Scenic> heat = travelDao.getScenicTread();     // 调用 DAO 方法查询数据
        HashMap<String, List<Long>> map = new HashMap<>();  // 将景点以及这个景点对应的所有数据整合成一条数据
        for(Scenic scenic: heat) {
            String time = scenic.getTime();                 // 获取数据库每条数据的时间
            if (!times.contains(time)) {                    // 这个时间在 times 中是否存在
                times.add(time);                            // 不存在则添加
            }
            String scenic_name = scenic.getScenic();        // 获取景点名称
            // 判断在 Map 中是否有 key, 并判断:
            // 如果有这个 key, 则把数据添加到 value 中
            // 如果没有这个 key, 则添加这个 key, 新添加一个集合
            map.computeIfAbsent(scenic_name, k->new ArrayList<>()).add(scenic.getCount());

            // 上面一行等同于下面的逻辑
            // if (map.containsKey(scienc)){
            //     map.get(scienc).add(scenic.getCount());
            // } else {
            //     List list=new ArrayList<Long>();
            //     list.add(scenic.getCount());
            //     map.put(scienc,list);
            // }
        }

        // System.out.println(times);
        // System.out.println(map);

        map.forEach((key, value) -> {
            LineData data = new LineData();
            data.setName(key);
            data.setData(value);
            series.add(data);
        });

        maps.put("time",    times);
        maps.put("series", series);
        return maps;
    }

    @Override
    public List<Scenic> getHeatDataByDateAndTime(String fromDateAndTime, String toDateAndTime) {
        return travelDao.getHeatDataByDateAndTime(fromDateAndTime, toDateAndTime);
    }

    @Override
    public List<Scenic> getScenicDataByDateAndTime(String fromDateAndTime, String toDateAndTime) {
        return travelDao.getScenicDataByDateAndTime(fromDateAndTime, toDateAndTime);
    }

    @Override
    public HashMap<String, Object> getScenicTreadByDateAndTime(String fromDateAndTime, String toDateAndTime) {
        return null;
    }

}
