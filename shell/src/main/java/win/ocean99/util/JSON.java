package win.ocean99.util;

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson2.JSONObject;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class JSON {

    public static JSONObject parseJsonFile(File jsonFile){
        String jsonString = FileUtil.readString(jsonFile, StandardCharsets.UTF_8);
        return JSONObject.parseObject(jsonString);
    }
}
