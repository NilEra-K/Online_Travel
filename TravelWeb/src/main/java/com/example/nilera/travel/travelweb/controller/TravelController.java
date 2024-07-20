package com.example.nilera.travel.travelweb.controller;

import com.example.nilera.travel.travelweb.entity.Scenic;
import com.example.nilera.travel.travelweb.services.TravelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController                 // 注解: 定义这个类是一个 Controller
@RequestMapping("/scenic")      // 注解: 定义访问这个类的 URL
// 访问 URL: http://ip:port/scenic
public class TravelController {
    @Autowired
    private TravelService travelService;

    // @RequestMapping 表示既可以通过 GET 访问, 也可以通过 POST 访问
    // @GetMapping     表示只可以通过 GET 访问
    // @PostMapping    表示只可以通过 POST 访问
    /**
     * @brief   表示获取一天访客的总数量
     * @url     http://localhost:9090/travelserver/scenic/getDaySum
     * @return
     */
    @RequestMapping("/getDaySum")
    public long getDaySum() {
        long sum = travelService.getDaySum();
        return sum;
    }

    @RequestMapping("/getCurrentSum")
    public long getCurrentSum() {
        long sum = travelService.getCurrentSum();
        return sum;
    }

    @RequestMapping("/getHeatData")
    public List<Scenic> getHeatData() {
        return travelService.getHeatData();
    }

    @RequestMapping("/getScenicMinuteData")
    public List<Scenic> getScenicMinuteData() {
        return travelService.getScenicMinuteData();
    }

    @RequestMapping("/getScenicTrend")
    public HashMap<String, Object> getScenicTrend() {
        return travelService.getScenicTread();
    }

    @PostMapping("/selectForm")
    @ResponseBody
    public Object receiveFormData(@RequestBody Map<String, String> formData) {
        // 在这里可以处理收到的formData
        // System.out.println(formData);
        String selectDate = formData.get("date");
        String selectFromTime = formData.get("fromTime");
        String selectToTime = formData.get("toTime");
        String selectTableName = formData.get("tableName");
        // System.out.println(selectDate + " " + selectFromTime + " " + selectToTime + " " + selectTableName);

        // 执行业务逻辑
        if (selectDate.equals("")) {
            return null;
        }
        String fromDateAndTime = selectDate.replace("-", "") + selectFromTime.replace(":", "").replace("：", "");
        String toDateAndTime = selectDate.replace("-", "") + selectToTime.replace(":", "").replace("：", "");
        System.out.println("From Date And Time: " + fromDateAndTime + "\nTo Date And Time: " + toDateAndTime);
        switch (selectTableName) {
            case "0": {
                System.out.println("请进行选择");
                break;
            }
            case "1": { // 热力图
                return travelService.getHeatDataByDateAndTime(fromDateAndTime, toDateAndTime);
            }
            case "2": { // 人流量柱状图
                return travelService.getScenicDataByDateAndTime(fromDateAndTime, toDateAndTime);
            }
            case "3": { // 人流量趋势图
                return travelService.getScenicTreadByDateAndTime(fromDateAndTime, toDateAndTime);
            }
            default:    // 未定义图表
                break;
        }
        return "Data received successfully";
    }
}
