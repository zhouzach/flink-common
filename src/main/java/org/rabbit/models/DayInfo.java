package org.rabbit.models;

public class DayInfo {
    private Long ts;
    private String category;
    private Long value;

    public Long getTs() {
        return ts;
    }

    public String getCategory() {
        return category;
    }

    public Long getValue() {
        return value;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public DayInfo(Long ts, String category, Long value) {
        this.ts = ts;
        this.category = category;
        this.value = value;
    }

    public DayInfo() {
    }
}
