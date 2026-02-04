package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.BlogDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.SystemConstants;
import cn.hutool.core.bean.BeanUtil;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IBlogService blogService;

    @Resource
    private UserServiceImpl userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据，并封装为 BlogDTO，同时设置是否点赞
        List<BlogDTO> list = page.getRecords().stream()
                .map(this::toBlogDTO)
                .peek(this::isBlogLiked)
                .collect(Collectors.toList());
        return Result.ok(list);
    }

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = blogService.getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        BlogDTO blogDTO = toBlogDTO(blog);
        isBlogLiked(blogDTO);
        return Result.ok(blogDTO);
    }

    private BlogDTO toBlogDTO(Blog blog) {
        BlogDTO blogDTO = BeanUtil.copyProperties(blog, BlogDTO.class);
        User user = userService.getById(blog.getUserId());
        blogDTO.setName(user.getNickName());
        blogDTO.setIcon(user.getIcon());
        return blogDTO;
    }

    private void isBlogLiked(BlogDTO blogDTO) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return;
        }
        Long userId = UserHolder.getUser().getId();
        // 判断用户是否点过赞
        String blogKey = BLOG_LIKED_KEY + blogDTO.getId().toString();
        Double score = stringRedisTemplate.opsForZSet().score(blogKey, userId.toString());
        blogDTO.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        // 获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 判断用户是否已经点赞
        String blogKey = BLOG_LIKED_KEY + id.toString();
        Double score = stringRedisTemplate.opsForZSet().score(blogKey, userId.toString());
        // 未点赞
        if (score == null) {
            // 数据库点赞数 + 1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 保存用户到 Redis 集合
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(blogKey, userId.toString(), System.currentTimeMillis());
            }
        }
        // 已点赞
        else {
            // 数据库点赞数 - 1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            // 从 Redis 集合中移除用户
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(blogKey, userId.toString());
            }
        }
        return null;
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 查询 top 5 点赞用户 zrange key 0 4
        String key = BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        // 解析其中 id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        // 根据 id 查找用户
        // order by field
        String idStr = StrUtil.join(",", ids);
        List<UserDTO> userDTOs = userService.query()
                .in("id", ids).last("order by filed(id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOs);
    }
}
