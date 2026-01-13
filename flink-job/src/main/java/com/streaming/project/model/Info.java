package com.streaming.project.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Info implements Serializable {
    private String symbol;
    private String company;
    private String sector;
    private Long market_cap;
    private Long info_timestamp;
}