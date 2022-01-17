package com.atguigu.flink.util;

import java.util.ArrayList;
import java.util.List;

/**
 * 定义一个迭代器转集合的工具类(由于多次需要用到)
 *
 * @author Evan
 * @ClassName AtguiguUtil
 * @date 2022-01-17 16:32
 */
public class AtguiguUtil {
    public static <T> List<T> toList(Iterable<T> it){
        ArrayList<T> result = new ArrayList<>();
        for (T t : it) {
            result.add(t);
        }

        return result;
    }
}
