package com.qiankun.mysql.controller;

/**
 * @Description:
 * @Date : 2023/11/09 16:33
 * @Auther : tiankun
 */
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.dest.AbstractProcessor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping(value = "/mysql/monitor",produces = {MediaType.APPLICATION_JSON_VALUE})
public class ViewController {

    @GetMapping("/viewChange")
    public Map viewChange(String modelId){
        if(StringUtils.isEmpty(modelId) || Replicator.replicator == null || Replicator.replicator.getProcessor() == null){
            return null;
        }
        AbstractProcessor processor = Replicator.replicator.getProcessor();
        Map<String,Object> result = new HashMap<>();
        result.put("data",processor.viewChange(modelId));
        return result;
    }
}
