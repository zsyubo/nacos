package org.example.controller;


import lombok.extern.slf4j.Slf4j;
import org.example.pojo.UserInfoVO;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/api/user/v1")
public class UserController {


    @PostMapping("/getUserInfo")
    @ResponseBody
    public UserInfoVO getUserInfo(Integer id) {
        UserInfoVO vo = new UserInfoVO();
        vo.setName("牛爷爷"+id);
        return vo;
    }
}
