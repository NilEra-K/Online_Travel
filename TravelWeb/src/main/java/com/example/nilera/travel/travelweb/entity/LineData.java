package com.example.nilera.travel.travelweb.entity;

import lombok.Data;

import java.util.List;

@Data
public class LineData {
    private String      name;
    private List<Long>  data;
}
