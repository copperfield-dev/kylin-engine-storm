package org.apache.kylin.engine.storm.StepBolts;

import org.apache.kylin.engine.storm.IStormInput.IJsonInputFormat;
import org.apache.kylin.engine.storm.common.StreamConstants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by copperfield on 22/01/2017.
 */
public class BaseCuboidBolt extends CuboidBoltBase {
    
    private final static Logger logger = LoggerFactory.getLogger(BaseCuboidBolt.class);
    
    private IJsonInputFormat flatTableInputFormat;
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        counter++;
        if (counter % StreamConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + counter + " records!");
        }
        
        try {
            //put a record into the shared bytesSplitter
            String[] row = flatTableInputFormat.parseJsonInput(tuple.getValue(0));
            bytesSplitter.setBuffers(convertUTF8Bytes(row));
            //take care of the data in bytesSplitter
            outputKV(basicOutputCollector);
        } catch (Exception ex) {
            handleErrorRecord(ex);
        }
    }
}
