package com.bupt.spark.Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * Created by guoxingyu on 2018/11/18.
 */
public class ConfigManager implements Serializable {
    private Properties prop = new Properties();

    /**
     *
     * @param propName
     * @throws IOException
     */
    public ConfigManager(String propName) throws IOException {
        FileInputStream in = null;
        try {
            in = new FileInputStream("src/main/resources/" + propName + ".properties");
            this.prop.load(in);
            System.out.println("读取配置文件信息成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("读取配置文件信息失败");
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    /**
     * 获取key的value
     * @param key
     * @return
     */
    public String getProperty(String key) {
        if (this.prop != null) {
            return this.prop.getProperty(key);
        } else {
            return null;
        }
    }
}
