# jms-error-demo

```java
public static class ByteStringMessageConverter extends SimpleMessageConverter {
        @Override
        public Object fromMessage(Message message) throws JMSException, MessageConversionException {
            Object obj = super.fromMessage(message);
            if (obj instanceof byte[]) {
                return new String((byte[])obj, Charset.defaultCharset());
            }
            return obj;
        }
} 
```    
