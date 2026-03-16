package io.rcktapp.api.handler.dynamo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoV2Utils {


   public static AttributeValue toAttributeValue(Object value)
   {
      if (value == null)
      {
         return AttributeValue.builder().nul(true).build();
      }
      if (value instanceof String)
      {
         return AttributeValue.builder().s((String) value).build();
      }
      if (value instanceof Number)
      {
         return AttributeValue.builder().n(value.toString()).build();
      }
      if (value instanceof Boolean)
      {
         return AttributeValue.builder().bool((Boolean) value).build();
      }
      if (value instanceof List)
      {
         List<AttributeValue> list = ((List<?>) value).stream()
                                                       .map(DynamoV2Utils::toAttributeValue)
                                                       .collect(Collectors.toList());
         return AttributeValue.builder().l(list).build();
      }
      if (value instanceof Map)
      {
         Map<String, AttributeValue> map = new HashMap<>();
         ((Map<?, ?>) value).forEach((k, v) -> map.put(k.toString(), toAttributeValue(v)));
         return AttributeValue.builder().m(map).build();
      }
      return AttributeValue.builder().s(value.toString()).build();
   }

   public static Map<String, AttributeValue> toItemMap(Map<String, Object> map)
   {
      Map<String, AttributeValue> item = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet())
      {
         item.put(entry.getKey(), toAttributeValue(entry.getValue()));
      }
      return item;
   }

   public static Map<String, Object> fromItemMap(Map<String, AttributeValue> item)
   {
      Map<String, Object> map = new HashMap<>();
      for (Map.Entry<String, AttributeValue> entry : item.entrySet())
      {
         map.put(entry.getKey(), fromAttributeValue(entry.getValue()));
      }
      return map;
   }

   public static Map<String, AttributeValue> toExpressionAttributeValues(Map<String, Object> args)
   {
      Map<String, AttributeValue> result = new HashMap<>();
      for (Map.Entry<String, Object> entry : args.entrySet())
      {
         result.put(entry.getKey(), toAttributeValue(entry.getValue()));
      }
      return result;
   }

   private static Object fromAttributeValue(AttributeValue av)
   {
      switch (av.type())
      {
         case S:
            return av.s();
         case N:
            try
            {
               return Long.parseLong(av.n());
            }
            catch (NumberFormatException e)
            {
               return Double.parseDouble(av.n());
            }
         case BOOL:
            return av.bool();
         case NUL:
            return null;
         case L:
            return av.l().stream()
                     .map(DynamoV2Utils::fromAttributeValue)
                     .collect(Collectors.toList());
         case M:
            return fromItemMap(av.m());
         default:
            return null;
      }
   }

}
