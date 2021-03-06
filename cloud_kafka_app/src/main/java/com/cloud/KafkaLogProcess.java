package com.cloud;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * kafka日志处理类。
 * @author haruluya
 */
public class KafkaLogProcess implements Processor<byte[], byte[]>{
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] dummy, byte[] line) {
        String input = new String(line);
        // 提取数据，以固定前缀过滤日志信息
        if( input.contains("PRODUCT_RATING_PREFIX:") ){
            System.out.println("product rating data coming! " + input);
            input = input.split("PRODUCT_RATING_PREFIX:")[1].trim();
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
