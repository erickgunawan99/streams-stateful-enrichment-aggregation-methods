package com.streaming.project.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Trade implements Serializable {
    private String symbol;
    private BigDecimal price;
    private Integer volume;
    private Long trade_timestamp;
}