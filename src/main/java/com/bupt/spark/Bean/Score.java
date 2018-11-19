package com.bupt.spark.Bean;

import java.io.Serializable;

/**
 * Created by guoxingyu on 2018/11/18.
 */
public class Score implements Serializable {

    private String id;
    private String name;
    private String subject;
    private String score;

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

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }
}
