package org.example.feign;


import org.example.pojo.UserInfoVO;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(value = "cloudExp")
public interface UserIntFeignClient {

    @PostMapping("/api/user/v1/getUserInfo")
    UserInfoVO getUserInfo(@RequestParam("id") Integer id);

}
