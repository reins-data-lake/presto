package com.facebook.presto.reinsApi.dto;

import lombok.Data;

import java.util.List;

@Data
public class SearchTableFilterDTO {
    List<Long> idList;
    long startTime;
    long endTime;
    List<FieldFilter> filters;
    @Data
    public class FieldFilter {
        String field;
        String op;
        String val1;
        String val2;
    }
}