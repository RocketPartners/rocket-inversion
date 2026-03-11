package io.rcktapp.api.handler.dynamo;

public class KeyConditionFragment
{
   String expression;
   String nameKey;
   String nameValue;
   String valueKey;
   Object valueObject;

   public KeyConditionFragment(String expression, String nameKey, String nameValue, String valueKey, Object valueObject)
   {
      this.expression = expression;
      this.nameKey = nameKey;
      this.nameValue = nameValue;
      this.valueKey = valueKey;
      this.valueObject = valueObject;
   }

   public String getExpression()
   {
      return expression;
   }

   public String getNameKey()
   {
      return nameKey;
   }

   public String getNameValue()
   {
      return nameValue;
   }

   public String getValueKey()
   {
      return valueKey;
   }

   public Object getValueObject()
   {
      return valueObject;
   }
}
