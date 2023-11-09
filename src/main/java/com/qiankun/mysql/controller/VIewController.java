package com.qiankun.mysql.controller;

/**
 * @Description:
 * @Date : 2023/11/09 16:33
 * @Auther : tiankun
 */
import com.qiankun.mysql.Replicator;
import com.qiankun.mysql.dest.AbstractProcessor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/mysql/monitor")
public class VIewController {

    @GetMapping("/viewChange")
    public Object viewChange(String modelId){
        if(StringUtils.isEmpty(modelId) || Replicator.replicator == null || Replicator.replicator.getProcessor() == null){
            return null;
        }
        AbstractProcessor processor = Replicator.replicator.getProcessor();
        return processor.viewChange(modelId);
    }
}
