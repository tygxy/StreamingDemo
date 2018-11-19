package com.bupt.spark.Handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bupt.spark.Bean.Output;
import com.bupt.spark.Bean.Score;
import com.bupt.spark.Bean.Student;
import com.bupt.spark.Utils.ConfigManager;

import java.io.Serializable;

/**
 * Created by guoxingyu on 2018/11/18.
 */
public class MsgHandler implements Serializable {

    /**
     * 检查json格式是否正确
     * @param jsonStr
     * @return
     */
    private boolean checkJsonFormat(String jsonStr) {
        if (jsonStr != null) {
            try {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("json格式异常");
                return false;
            }
        } else {
            return false;
        }
    }

    /**
     * 判断json是否包含必备字段的key
     * @param jsonObject
     * @param fieldsList
     * @return
     */
    private boolean checkMustFieldKeysExist(JSONObject jsonObject, String[] fieldsList) {
        if (jsonObject != null && fieldsList.length > 0) {
            for (String field : fieldsList) {
                if (!jsonObject.containsKey(field)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 判断json必备字段的value是否存在
     * @param jsonObject
     * @param fieldsList
     * @return
     */
    private boolean checkMustFieldValueExist(JSONObject jsonObject, String[] fieldsList) {
        if (jsonObject != null && fieldsList.length > 0) {
            for (String field : fieldsList) {
                if (ifInvaildValue(jsonObject.get(field).toString())) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }

    }

    /**
     * 检查json基本信息，包括格式，必备字段，必备字段是否有值
     * @param jsonStr
     * @return
     */
    public boolean checkAll(String jsonStr,ConfigManager configManager) {
        // 检查json格式是否正确
        if (!checkJsonFormat(jsonStr)) {
            return false;
        }

        JSONObject jsonObject = JSON.parseObject(jsonStr);

        // 检查json是否包含必备字段
        if (!checkMustFieldKeysExist(jsonObject,getStringList(configManager.getProperty("must.fields.key")))) {
            return false;
        }

        // 检查必备字段的值是否缺失
        if (!checkMustFieldValueExist(jsonObject,getStringList(configManager.getProperty("must.fields.value")))) {
            return false;
        }

        return true;
    }

    /**
     * 筛选出分数大于等于60
     * @param score
     * @return
     */
    private boolean pickScore(String score) {
        if (Integer.parseInt(score) >= 60) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 筛选Msg
     * @param jsonObject
     * @return
     */
    public boolean pickMsg(JSONObject jsonObject) {
        if (jsonObject != null) {
            if (!pickScore(jsonObject.getString("score"))) {
                return false;
            }
            return true;
        } else {
            return false;
        }

    }

    /**
     * 清洗并删选数据
     * @param jsonStr
     * @param configManager
     * @return
     */
    public boolean cleanAndPickUpMsg(String jsonStr,ConfigManager configManager) {
        if (jsonStr != null && configManager != null) {

            if (!checkAll(jsonStr,configManager)) {
                return false;
            }

            JSONObject jsonObject = JSON.parseObject(jsonStr);

            if (!pickMsg(jsonObject)) {
                return  false;
            }

            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取字符串列表
     * @param fieldsListStr
     * @return
     */
    private String[] getStringList(String fieldsListStr) {
        if (fieldsListStr != null) {
            return fieldsListStr.split(",");
        } else {
            return null;
        }
    }

    /**
     * 判断字段是否是无效值
     * @param value
     * @return
     */
    private boolean ifInvaildValue(String value) {
        if (value == null || value.equals("")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取ScoreBean
     * @param jsonStr
     * @return
     */
    public Score getScoreBean(String jsonStr) {
        try {
            Score score = JSON.parseObject(jsonStr,Score.class);
            return score;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("转换ScoreBean失败");
            return null;
        }
    }

    /**
     * 获取TeacherBean
     * @param jsonStr
     * @return
     */
    public Student getStudentBean(String jsonStr) {
        if (jsonStr != null) {
            try {
                Student student = JSON.parseObject(jsonStr,Student.class);
                return student;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("转换StudentBean失败");
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * 获取OutputBean
     * @param score
     * @param student
     * @return
     */
    public Output getOutputBean(Score score,Student student) {
        if (score != null && student != null) {
            Output output = new Output();

            output.setId(student.getId());
            output.setName(student.getName());
            output.setSex(student.getSex());
            output.setAge(student.getAge());
            output.setSubject(score.getSubject());
            output.setScore(score.getScore());

            return output;
        } else {
            return null;
        }
    }


}
