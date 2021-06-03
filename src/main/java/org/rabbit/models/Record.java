package org.rabbit.models;

public class Record {
    private String name;
    private int num;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public Record() {
    }

    public Record(String feild1, int feild2) {
        this.name = feild1;
        this.num = feild2;
    }

    @Override
    public String toString() {
        return "Record{" +
                "name='" + name + '\'' +
                ", num=" + num +
                '}';
    }
}
