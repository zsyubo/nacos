package org.example.controller;

import lombok.extern.slf4j.Slf4j;
import org.example.feign.UserIntFeignClient;
import org.example.pojo.UserInfoVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

//http://127.0.0.1:12021/user/getUserInfo?id=1
@RestController
@RequestMapping("/user/")
public class UserController {

    @Autowired
    private UserIntFeignClient userIntFeignClient;

    @RequestMapping("/getUserInfo")
    @ResponseBody
    public UserInfoVO getUserInfo(Integer id){
        return userIntFeignClient.getUserInfo(id);
    }
}
