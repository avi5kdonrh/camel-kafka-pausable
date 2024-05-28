package org.acme;

import org.apache.camel.component.kafka.consumer.errorhandler.KafkaConsumerListener;
import org.apache.camel.component.kafka.consumer.support.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySeekPolicy extends KafkaConsumerListener {
    private static final Logger LOG = LoggerFactory.getLogger(MySeekPolicy.class);

    @Override
    public boolean afterProcess(ProcessingResult result) {
        if (result.isFailed()) {
            LOG.warn("Pausing consumer due to error on the last processing");
            this.getConsumer().pause(this.getConsumer().assignment());
            LOG.info("CURRENTOFF>>>>>> "+this.getConsumer().committed(this.getConsumer().assignment()));
           this.getConsumer().assignment().forEach(topicPartition -> {
               long offset = this.getConsumer().committed(this.getConsumer().assignment()).get(topicPartition).offset();
               LOG.info("Seeking ....... "+offset);
               getConsumer().seek(topicPartition, offset);
           });
            return false;
        } else {
            return true;
        }
    }
}
