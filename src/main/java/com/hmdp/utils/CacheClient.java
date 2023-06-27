package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import static com.hmdp.utils.RedisConstants.*;


/**
 * @author shkstart
 * @create 2023--13-10:37
11211231
cc123
 11231
 9981
 112
 110
 */

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(value),time,unit);
    }

    public void setWithLogicalExpir(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    public <R,ID>  R queryWithPossThrough(String keyPrefix, ID id, Class<R> type,
                                          Function<ID,R> dbFallback,Long time, TimeUnit unit){
        String key=keyPrefix + id;
        //1-从Redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2-判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3-存在，返回商品信息
            return JSONUtil.toBean(json, type);
        }
        //判断命中的是否是空值
        if (json!=null){
            //返回一个错误信息
            return null;
        }
        //4-不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        //判断数据库是否查询成功
        //5-不存在
        if (r==null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            //返回404错误
            return null;
        }
        //6-存在，将数据写入Reids
        set(key,r,time,unit);
        //7-返回商品信息
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type,
                                           Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        String key=keyPrefix+ id;
        //1-从Redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2-判断是否存在
        if (StrUtil.isBlank(json)) {
            //3-不存在，返回null
            return null;
        }
        //4-命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())){
            //5.1-未过期，直接返回店铺信息
            return r;
        }
        //5-2已过期，根据缓存重建
        //6缓存重建
        //6.1-获取互斥锁
        String lockKey=LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        //6.2-判断是否获取锁成功
        if (isLock){
            //获取锁成功应该再次检查Redis缓存是否过期

            //6.3-成功，开启独立线程（线程池），实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    //重建缓存
                    R r1 = dbFallback.apply(id);
                    setWithLogicalExpir(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unlock(lockKey);
                }
            });
        }
        //6.4-返回过期的商铺信息
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }


}
