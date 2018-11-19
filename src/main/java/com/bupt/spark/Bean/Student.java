package com.bupt.spark.Bean;

import java.io.Serializable;

/**
 * Created by guoxingyu on 2018/11/19.
 */
public class Student implements Serializable {

    private String id;
    private String name;
    private String sex;
    private String age;

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }
}
