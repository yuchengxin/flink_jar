package com.catyee.test.common.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;

/** RowDataUtils. */
public class RowDataUtils {

    public static Object getData(RowData row, int fieldPos, @Nonnull LogicalType fieldType) {
        if (row == null || row.isNullAt(fieldPos)) {
            return null;
        }

        switch (fieldType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return row.getString(fieldPos).toString();
            case ARRAY:
                ArrayData arrayData = row.getArray(fieldPos);
                LogicalType elementType = ((ArrayType) fieldType).getElementType();
                return getArrayData(arrayData, elementType);
            case MULTISET:
                MapData multiSetData = row.getMap(fieldPos);
                return convertSetData(multiSetData, (MultisetType) fieldType);
            case MAP:
                MapData data = row.getMap(fieldPos);
                return convertMapData(data, (MapType) fieldType);
            case ROW:
                final int rowFieldCount = getFieldCount(fieldType);
                RowData elementData = row.getRow(fieldPos, rowFieldCount);
                return convertRowData(elementData, (RowType) fieldType);
            case STRUCTURED_TYPE:
                final int rowFieldCounts = getFieldCount(fieldType);
                RowData structData = row.getRow(fieldPos, rowFieldCounts);
                return convertStructData(structData, (StructuredType) fieldType);
            case TIMESTAMP_WITH_TIME_ZONE:
            case DISTINCT_TYPE:
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
                throw new UnsupportedOperationException(
                        "unsupported type when get value from RowData: " + fieldType);
            default:
                return RowData.createFieldGetter(fieldType, fieldPos).getFieldOrNull(row);
        }
    }

    public static List<Object> getArrayData(ArrayData arrayData, LogicalType elementType) {
        if (arrayData == null) {
            return new ArrayList<>();
        }
        // ordered by type root definition
        switch (elementType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return convertArrayData(arrayData, (row, pos) -> row.getString(pos).toString());
            case ARRAY:
                LogicalType type = ((ArrayType) elementType).getElementType();
                return convertArrayData(
                        arrayData,
                        (array, pos) -> {
                            ArrayData eleArrayData = array.getArray(pos);
                            return getArrayData(eleArrayData, type);
                        });
            case MULTISET:
                return convertArrayData(
                        arrayData,
                        (array, pos) -> {
                            MapData eleMapData = array.getMap(pos);
                            return convertSetData(eleMapData, (MultisetType) elementType);
                        });
            case MAP:
                return convertArrayData(
                        arrayData,
                        (array, pos) -> {
                            MapData mapData = array.getMap(pos);
                            return convertMapData(mapData, (MapType) elementType);
                        });
            case ROW:
                return convertArrayData(
                        arrayData,
                        (array, pos) -> {
                            int rowFieldCount = getFieldCount(elementType);
                            RowData elementData = array.getRow(pos, rowFieldCount);
                            return convertRowData(elementData, (RowType) elementType);
                        });
            case STRUCTURED_TYPE:
                return convertArrayData(
                        arrayData,
                        (array, pos) -> {
                            final int rowFieldCounts = getFieldCount(elementType);
                            RowData structData = array.getRow(pos, rowFieldCounts);
                            return convertStructData(structData, (StructuredType) elementType);
                        });
            case TIMESTAMP_WITH_TIME_ZONE:
            case DISTINCT_TYPE:
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
                throw new UnsupportedOperationException(
                        "unsupported type when get value from ArrayData: " + elementType);
            default:
                return convertArrayData(
                        arrayData,
                        (array, pos) ->
                                ArrayData.createElementGetter(elementType)
                                        .getElementOrNull(array, pos));
        }
    }

    public static <T> List<T> convertArrayData(
            ArrayData data, BiFunction<ArrayData, Integer, T> biFunction) {
        List<T> results = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            results.add(biFunction.apply(data, i));
        }
        return results;
    }

    private static List<Object> convertSetData(MapData multiSetData, MultisetType fieldType) {
        LogicalType setValueType = fieldType.getElementType();
        return getArrayData(multiSetData.valueArray(), setValueType);
    }

    private static Map<Object, Object> convertMapData(MapData data, MapType fieldType) {
        ArrayData keyArray = data.keyArray();
        LogicalType keyType = fieldType.getKeyType();
        List<Object> keyList = getArrayData(keyArray, keyType);
        ArrayData valueArray = data.valueArray();
        LogicalType valueType = fieldType.getValueType();
        List<Object> valueList = getArrayData(valueArray, valueType);
        Map<Object, Object> results = new HashMap<>();
        for (int i = 0; i < keyList.size(); i++) {
            Object key = keyList.get(i);
            Object value = valueList.get(i);
            results.put(key, value);
        }
        return results;
    }

    private static Map<String, Object> convertRowData(RowData rowData, RowType fieldType) {
        List<RowType.RowField> rowFields = fieldType.getFields();
        Map<String, Object> rowMap = new HashMap<>();
        for (int i = 0; i < rowFields.size(); i++) {
            RowType.RowField field = rowFields.get(i);
            LogicalType type = field.getType();
            Object value = getData(rowData, i, type);
            rowMap.put(field.getName(), value);
        }
        return rowMap;
    }

    private static Map<String, Object> convertStructData(
            RowData structData, StructuredType fieldType) {
        List<StructuredType.StructuredAttribute> attributes = fieldType.getAttributes();
        Map<String, Object> structMap = new HashMap<>();
        for (int i = 0; i < attributes.size(); i++) {
            StructuredType.StructuredAttribute field = attributes.get(i);
            LogicalType type = field.getType();
            Object value = getData(structData, i, type);
            structMap.put(field.getName(), value);
        }
        return structMap;
    }
}
