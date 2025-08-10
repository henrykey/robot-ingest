package com.ibcai.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.hash.Hashing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 工具类：用于从 JSON 字符串提取核心字段，量化 position，生成 Murmur3_128 哈希。
 */
public class JsonCoreHasher {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * 计算核心哈希
     * @param jsonStr 原始 JSON 字符串
     * @param coreFields 需要提取的字段名列表
     * @param posDecimals position.lat/lon 量化小数位数（如无则不处理）
     * @return Murmur3_128 哈希的十六进制字符串
     */
    public static String coreHash(String jsonStr, List<String> coreFields, int posDecimals) {
        try {
            if (coreFields == null || coreFields.isEmpty()) {
                // 只按时间窗
                return Hashing.murmur3_128().hashString("{}", StandardCharsets.UTF_8).toString();
            }
            JsonNode root = mapper.readTree(jsonStr);
            ObjectNode coreObj = mapper.createObjectNode();
            for (String field : coreFields) {
                if (field.startsWith("position.")) {
                    // 量化 position.lat/lon
                    String subField = field.substring("position.".length());
                    JsonNode posNode = root.path("position").path(subField);
                    if (!posNode.isMissingNode() && posNode.isNumber()) {
                        BigDecimal val = posNode.decimalValue();
                        BigDecimal quantized = val.setScale(posDecimals, RoundingMode.HALF_UP);
                        coreObj.with("position").put(subField, quantized);
                    }
                } else {
                    JsonNode node = root.path(field);
                    if (!node.isMissingNode()) {
                        coreObj.set(field, node);
                    }
                }
            }
            String normJson = mapper.writeValueAsString(coreObj);
            return Hashing.murmur3_128().hashString(normJson, StandardCharsets.UTF_8).toString();
        } catch (Exception e) {
            // 异常时返回空对象哈希
            return Hashing.murmur3_128().hashString("{}", StandardCharsets.UTF_8).toString();
        }
    }
}
