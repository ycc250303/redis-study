package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.LOCK_ORDER_KEY;


/**
 * 服务实现类
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @Autowired
    private VoucherMapper voucherMapper;

    /**
     * 检查并创建 stream 与消费者组
     */
    private void initStreamAndGroup() {
        String streamKey = "stream.orders";
        String groupName = "g1";
        try {
            // 尝试创建消费者组，如果 stream 不存在或组已存在都会抛异常
            stringRedisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), groupName);
        } catch (Exception e) {
            String msg = e.getMessage();
            // BUSYGROUP 代表组已存在，忽略；其他异常按需记录日志
            if (msg != null && msg.contains("BUSYGROUP")) {
                // do nothing
            } else {
                // 这里不再继续抛出，避免影响项目启动
                // 如需排查，可临时打开日志
                // log.error("初始化 stream 与消费者组失败", e);
            }
        }
    }

    @PostConstruct
    private void init() {
        // 先检查并创建 stream.orders 与 g1 消费者组，避免后续 XREADGROUP 报 NOGROUP
        initStreamAndGroup();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS seckill:orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断获取消息是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1 获取失败，循环
                        continue;
                    }

                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.获取成功，下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认
                    // XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    handlePendingList();
                    log.error("处理订单异常", e);
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS seckill:orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2.判断获取消息是否成功
                    if (list == null || list.isEmpty()) {
                        // 2.1 获取失败，说明没有消息，结束循环
                        break;
                    }

                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.获取成功，下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认
                    // XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    String msg = e.getMessage();
                    // 如果是 NOGROUP 错误，说明 stream 或消费者组不存在，尝试重新初始化一次
                    if (msg != null && msg.contains("NOGROUP")) {
                        initStreamAndGroup();
                    }
                    log.error("处理pending-list订单异常", e);
                    try{
                        Thread.sleep(20);
                    }catch (InterruptedException interruptedException){
                        interruptedException.printStackTrace();
                    }

                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 创建锁对象
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock(LOCK_ORDER_KEY + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        if (!isLock) {
            // 失败
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;


    @Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        // 1.执行lua脚本
        // 获取用户 id
        Long userId = UserHolder.getUser().getId();
        // 获取订单 id
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        // 2.判断结果是否为 0
        int r = result.intValue();
        // 2.1 不为 0 没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 3.返回订单 ID
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        // 5.一人一单，根据优惠券 id 和用户 id 查询订单是否存在
        int count = query().eq("voucher_id", voucherOrder.getVoucherId())
                .eq("user_id", userId)
                .count();
        if (count > 0) {
            log.error("用户已经购买过一次");
            return;
        }
        // 6.扣减库存
        // 乐观锁方式
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }

        save(voucherOrder);
    }
}
