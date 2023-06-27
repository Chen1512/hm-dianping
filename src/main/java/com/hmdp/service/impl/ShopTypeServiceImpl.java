package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.interfaces.Func;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.controller.ShopTypeController;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryList() {
        List<ShopType> shopTypeList;
        //1-先查询Redis缓存中的shoptype
        String shoptypes = stringRedisTemplate.opsForValue().get("shop:type");
        //2-判断是否存在
        if (StrUtil.isNotBlank(shoptypes)) {
            //3-存在，进行返回
             shopTypeList= JSONUtil.toList(shoptypes, ShopType.class);
            return Result.ok(shopTypeList);
        }
        //4-不存在，在数据库中查找
        shopTypeList = query().orderByAsc("sort").list();
        //判断数据库是否查询成功
        if (shopTypeList.isEmpty()){
            //5-不纯在，返回404错误
            return Result.fail("找不到这个类型");
        }
        //6-存在，把数据保存到Redis中
        stringRedisTemplate.opsForValue().set("shop:type",JSONUtil.toJsonStr(shopTypeList),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7-进行返回
        return Result.ok(shopTypeList);
    }
}
