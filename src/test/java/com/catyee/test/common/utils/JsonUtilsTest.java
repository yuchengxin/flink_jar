package com.catyee.test.common.utils;


import com.catyee.test.common.entity.ParTableEntry;
import org.junit.Test;

public class JsonUtilsTest {

    @Test
    public void getJsonString() {
        ParTableEntry entry = FraudUtils.fraudParTableEntry(1, "user");
        String json = JsonUtils.getJsonString(entry);
        System.out.println(json);
    }
}