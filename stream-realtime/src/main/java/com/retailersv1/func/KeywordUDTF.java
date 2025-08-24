package com.retailersv1.func;


import com.stream.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
/**
 * Title: KeywordUDTF
 * Author: chenglong
 * Package: com.retailersv1.func
 * Date: 2025/8/24 19:39
 * Description:
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF  extends TableFunction<Row> {
    public void eval(String test) {
        for (String keyword : KeywordUtil.analyze(test)) {
            collect(Row.of(keyword));
        }
    }
}
