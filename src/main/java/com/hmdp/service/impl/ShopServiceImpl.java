package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.NonNull;
import netscape.javascript.JSUtil;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = cacheClient.queryWithPossThrough(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        //互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);

        //逻辑过期解决缓存击穿
        //Shop shop = queryWithLogicalExpire(id);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop==null){
            Result.fail("店铺不存在！");
        }
        //7-返回商品信息
        return Result.ok(shop);
    }
    private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
/**
 * @Description:缓存击穿问题
 * @return:
 * @author: chen
 * @date: 2023/5/13 13:30
 */
    //public Shop queryWithLogicalExpire(Long id){
    //    String key=CACHE_SHOP_KEY + id;
    //    //1-从Redis中查询缓存
    //    String shopJson = stringRedisTemplate.opsForValue().get(key);
    //    //2-判断是否存在
    //    if (StrUtil.isBlank(shopJson)) {
    //        //3-不存在，返回null
    //        return null;
    //    }
    //    //4-命中，需要先把json反序列化为对象
    //    RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
    //    JSONObject data = (JSONObject) redisData.getData();
    //    Shop shop = JSONUtil.toBean(data, Shop.class);
    //    LocalDateTime expireTime = redisData.getExpireTime();
    //    //5判断是否过期
    //    if (expireTime.isAfter(LocalDateTime.now())){
    //        //5.1-未过期，直接返回店铺信息
    //        return shop;
    //    }
    //    //5-2已过期，根据缓存重建
    //    //6缓存重建
    //    //6.1-获取互斥锁
    //    String lockKey=LOCK_SHOP_KEY+id;
    //    boolean isLock = tryLock(lockKey);
    //    //6.2-判断是否获取锁成功
    //    if (isLock){
    //        //获取锁成功应该再次检查Redis缓存是否过期
    //
    //        //6.3-成功，开启独立线程（线程池），实现缓存重建
    //        CACHE_REBUILD_EXECUTOR.submit(()->{
    //            try {
    //                //重建缓存
    //                saveShop2Redis(id,CACHE_SHOP_TTL);
    //            } catch (Exception e) {
    //                throw new RuntimeException(e);
    //            } finally {
    //                //释放锁
    //                unlock(lockKey);
    //            }
    //        });
    //    }
    //    //6.4-返回过期的商铺信息
    //    return shop;
    //}
    //
    //public Shop queryWithMutex(Long id){
    //    String lockKey= null;
    //    Shop shop = null;
    //    try {
    //        String key=CACHE_SHOP_KEY + id;
    //        //1-从Redis中查询缓存
    //        String shopJson = stringRedisTemplate.opsForValue().get(key);
    //        //2-判断是否存在
    //        if (StrUtil.isNotBlank(shopJson)) {
    //            //3-存在，返回商品信息
    //            shop = JSONUtil.toBean(shopJson, Shop.class);
    //            return shop;
    //        }
    //        //判断命中的是否是空值
    //        if (shopJson!=null){
    //            //返回一个错误信息
    //            return null;
    //        }
    //        //4-实现缓存重建
    //        lockKey = LOCK_SHOP_KEY + id;
    //        //4.1-获取互斥锁
    //        boolean isLock = tryLock(lockKey);
    //        //4.2-判断是否成功
    //        if (!isLock) {
    //            //4.3-失败，休眠
    //            Thread.sleep(50);
    //            //重试
    //            return queryWithMutex(id);
    //        }
    //
    //
    //        //4.4-成功根据id查询数据库
    //        // 不存在，根据id查询数据库
    //
    //        //获取互斥锁成功应该再检测redis缓存是否存在，如果存在则无需重建缓存
    //        shop = getById(id);
    //        //模拟重建延时
    //        //Thread.sleep(200);
    //        //判断数据库是否查询成功
    //        //5-不存在
    //        if (shop==null){
    //            //将空值写入redis
    //            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
    //            //返回404错误
    //            return null;
    //        }
    //        //6-存在，将数据写入Reids
    //        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
    //    } catch (InterruptedException e) {
    //        throw new RuntimeException();
    //    }finally {
    //        //7.1-释放互斥锁
    //        unlock(lockKey);
    //    }
    //    //7-返回商品信息
    //    return shop;
    //}
    //public Shop queryWithPossThrough(Long id){
    //    String key=CACHE_SHOP_KEY + id;
    //    //1-从Redis中查询缓存
    //    String shopJson = stringRedisTemplate.opsForValue().get(key);
    //    //2-判断是否存在
    //    if (StrUtil.isNotBlank(shopJson)) {
    //        //3-存在，返回商品信息
    //        Shop shop = JSONUtil.toBean(shopJson, Shop.class);
    //        return shop;
    //    }
    //    //判断命中的是否是空值
    //    if (shopJson!=null){
    //        //返回一个错误信息
    //        return null;
    //    }
    //    //4-不存在，根据id查询数据库
    //    Shop shop = getById(id);
    //    //判断数据库是否查询成功
    //    //5-不存在
    //    if (shop==null){
    //        //将空值写入redis
    //        stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
    //        //返回404错误
    //        return null;
    //    }
    //    //6-存在，将数据写入Reids
    //    stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
    //    //7-返回商品信息
    //    return shop;
    //}

    //private boolean tryLock(String key){
    //    Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.MINUTES);
    //    return BooleanUtil.isTrue(flag);
    //}
    //
    //private void unlock(String key){
    //    stringRedisTemplate.delete(key);
    //}
    //


    /**
     * @Description:解决缓存穿透问题
     * @return: com.hmdp.entity.Shop
     * @author: chen
     * @date: 2023/5/13 11:05
     */


    public void saveShop2Redis(long id,Long expireSeconds){
        //1-查询店铺数据
        Shop shop = getById(id);
        //2-封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //3-写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        //Thread.sleep(200);
        if (id==null) {
            return Result.fail("店铺id不能为空");
        }
        //1-更新数据库
        updateById(shop);
        //2-删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1-判断是否需要根据坐标查询
        if (x==null||y==null){
            //不需要坐标查询，按数据库查
            Page<Shop> page = query().eq("type_id", typeId).page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            //返回数据
            return Result.ok(page.getRecords());
        }
        //2-计算分页参数
        int from=(current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end=current*SystemConstants.DEFAULT_PAGE_SIZE;
        //3-查询Redis，按距离排序，分页。结果：shopId，distance
        String key=SHOP_GEO_KEY+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key, GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));
        //4-解析出id
        if (results==null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        //4.1-截取from~end部分
        ArrayList<Long> ids = new ArrayList<>(list.size());
        HashMap<String, Distance> distanceMap = new HashMap<>(list.size());
        if (list.size()<=from){
            return Result.ok(Collections.emptyList());
        }
        list.stream().skip(from).forEach(result->{
            //4.2获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //4.3-获取距离
            @NonNull Distance distance = result.getDistance();
            distanceMap.put(shopIdStr,distance);

        });
        //5-根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        //6-返回
        return Result.ok(shops);
    }


}
