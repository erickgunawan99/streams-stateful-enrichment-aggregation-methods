package com.streaming.project.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedTrade implements Serializable {
    // Trade Fields
    private String symbol;
    private BigDecimal price;
    private Integer volume;
    private Long trade_timestamp; // Use Long for Flink Watermarks
    
    // Info Fields
    private String company;
    private String sector;
    private Long market_cap;
    private Long info_timestamp;
    
    // Calculated Field
    private BigDecimal trade_value;
}