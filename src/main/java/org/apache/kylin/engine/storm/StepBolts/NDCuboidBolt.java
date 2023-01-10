package org.apache.kylin.engine.storm.StepBolts;

import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.storm.common.StreamConstants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by copperfield on 22/01/2017.
 */
public class NDCuboidBolt extends BaseBasicBolt {
    
    private static final Logger logger = LoggerFactory.getLogger(NDCuboidBolt.class);
    
    private String cubeName;
    private CubeDesc cubeDesc;
    private CuboidScheduler cuboidScheduler;
    
    private int handleCounter;
    
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        long cuboidId = 1L;    // Think how to get it from tuple
    
        Cuboid parentCuboid = Cuboid.findById(cubeDesc, cuboidId);
    
        Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);
    
        handleCounter++;
        if (handleCounter % StreamConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
            logger.info("Handled " + handleCounter + " records!");
        }
        
        for (Long child : myChildren) {
            Cuboid childCuboid = Cuboid.findById(cubeDesc, child);
            
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        
    }
    
    
}
