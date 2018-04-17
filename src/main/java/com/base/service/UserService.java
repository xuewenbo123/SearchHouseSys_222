package com.base.service;

import com.base.model.User;

import java.util.List;

public interface  UserService {

    int addUser(User user);

    List<User> findAllUser(int pageNum, int pageSize);

    User findUserById(Integer id);
}
