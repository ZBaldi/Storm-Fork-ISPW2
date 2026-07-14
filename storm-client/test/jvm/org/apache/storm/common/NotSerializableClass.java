package org.apache.storm.common;

public class NotSerializableClass {

    private String string;

    public String getString() {

        return string;
    }
    public void setString(String string) {

        this.string = string;
    }
}
