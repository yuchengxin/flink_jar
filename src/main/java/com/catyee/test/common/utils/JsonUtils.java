package com.catyee.test.common.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalTimeSerializer;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JsonUtils {
    public static final TypeReference<List<String>> LIST_STRING_TR = new TypeReference<List<String>>() {};

    @Getter
    private static final ObjectMapper objMapper;

    static {
        objMapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));

        javaTimeModule.addSerializer(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        javaTimeModule.addDeserializer(LocalDate.class, new LocalDateDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));

        javaTimeModule.addSerializer(LocalTime.class, new LocalTimeSerializer(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));
        javaTimeModule.addDeserializer(LocalTime.class, new LocalTimeDeserializer(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")));

        objMapper.registerModule(javaTimeModule);
    }

    public static <T> T getObject(InputStream in, TypeReference<T> type) {
        if (in == null) {
            return null;
        }
        return FunctionUtils.call(() -> objMapper.readValue(in, type));
    }

    public static <T> T getObject(String jsonStr, TypeReference<T> type) {
        if (StringUtils.isBlank(jsonStr)) {
            return null;
        }
        return FunctionUtils.call(() -> objMapper.readValue(jsonStr, type));
    }

    public static <T> T getObject(byte[] bytes, TypeReference<T> type) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return FunctionUtils.call(() -> objMapper.readValue(bytes, type));
    }

    public static String getJsonString(Object obj) {
        return FunctionUtils.call(() -> objMapper.writeValueAsString(obj));
    }

    public  static byte[] getJsonBytes(Object obj) {
        return FunctionUtils.call(() -> objMapper.writeValueAsBytes(obj));
    }
}
