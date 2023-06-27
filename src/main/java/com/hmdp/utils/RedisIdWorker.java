package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * @author shkstart
 * @create 2023--13-16:04
 */

@Component
public class RedisIdWorker {

    //开始时间戳
    private static final long BEGIN_TIMESTAMP=1640995200L;
    private static final int COUNT_BTTS=32;

    //序列号位数
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix){
        //1-生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp=nowSecond-BEGIN_TIMESTAMP;

        //2-生成序列号
        //2.1-获取当前日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //3-拼接并返回
        return timestamp<<COUNT_BTTS | count;
    }

    //public static void main(String[] args) {
    //    LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
    //    System.out.println(time.toEpochSecond(ZoneOffset.UTC));
    //}
}
