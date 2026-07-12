package org.apache.storm.utils;

import java.io.Serializable;

public class SerializableClass implements Serializable {

    private String string;

    public SerializableClass(String string) {

        this.string = string;
    }

    public String getString() {

        return string;
    }
    public void setString(String string) {

        this.string = string;
    }
}
