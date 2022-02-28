package org.example.pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class UserInfoVO implements Serializable {
    private static final long serialVersionUID = 6623027884790671171L;

    private String name;
}
