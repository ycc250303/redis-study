package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    private final String sessionCode = "code";

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }

        String code = RandomUtil.randomNumbers(6);

        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        log.info("验证码发送成功，验证码: {}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }

        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.equals(code)) {
            return Result.fail("验证码错误或已过期");
        }

        User user = query().eq("phone", loginForm.getPhone()).one();
        if (user == null) {
            createUserWithPhone(phone);
            // 创建用户后重新查询
            user = query().eq("phone", loginForm.getPhone()).one();
        }

        String token = UUID.randomUUID().toString(true);
        String tokenKey = LOGIN_USER_KEY + token;
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> stringObjectMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor(
                                (fieldName, fieldValue) -> fieldValue == null ? null : fieldValue.toString()));

        stringRedisTemplate.opsForHash().putAll(tokenKey, stringObjectMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 获取当前日期
        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        // 拼接key
        String key = "sign:" + userId + ":" + now;
        // 获取今天是当月的第几天
        int offset = LocalDateTime.now().getDayOfMonth();
        // 写入Redis
        stringRedisTemplate.opsForValue().setBit(key, offset - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 获取当前日期
        String now = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        // 拼接key
        String key = "sign:" + userId + ":" + now;
        // 获取今天是当月的第几天
        int offset = LocalDateTime.now().getDayOfMonth();
        // 获取这个月到今天为止的所有的签到记录
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(offset)).valueAt(0)
        );
        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0L) {
            return Result.ok(0);
        }
        // 循环遍历
        int count = 0;
        // 未签到，结束
        // 签到，计数器加一
        while ((num & 1) != 0) {
            // 数字与 1 做与运算，得到数字最后一个 bit 位
            count++;
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private void createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
    }
}
