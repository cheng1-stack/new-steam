package com.stream.utils;

/**
 * Title: KeywordUtil
 * Author: chenglong
 * Package: com.stream.utils
 * Date: 2025/8/24 19:14
 * Description: key
 */

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


public class KeywordUtil {
    public static List<String> analyze(String text){
        StringReader reader = new StringReader(text);
        List<String> keywordList = new ArrayList<>();
        IKSegmenter ik = new IKSegmenter(reader, true);
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return keywordList;
    }
}