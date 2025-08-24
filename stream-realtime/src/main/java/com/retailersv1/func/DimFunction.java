package com.retailersv1.func;

/**
 * Title: DimFunction
 * Author: chenglong
 * Package: com.retailersv1.func
 * Date: 2025/8/24 19:10
 * Description: dim
 */
import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getRowKey(T bean);
    String getTableName();
    void addDims(T bean, JSONObject dim);
}