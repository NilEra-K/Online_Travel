package com.example.nilera.travel.travelweb.entity;

import lombok.Data;

@Data
public class Scenic {
    private double lng;
    private double lat;
    private long count;
    private String time;
    private String scenic;
}


--             p_title LIKE CONCAT('%', #{search}, '%')
--             OR p_abstract LIKE CONCAT('%', #{search}, '%')
--         LIMIT #{limit} OFFSET #{offset}