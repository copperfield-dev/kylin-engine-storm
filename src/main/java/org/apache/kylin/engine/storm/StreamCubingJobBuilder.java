/**
 * Created by copperfield on 22/01/2017.
 */

package org.apache.kylin.engine.storm;


import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.engine.storm.StepBolts.BaseCuboidBolt;
import org.apache.kylin.engine.storm.StepBolts.NDCuboidBolt;
import org.apache.kylin.engine.storm.common.AbstractStormJob;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamCubingJobBuilder {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamCubingJobBuilder.class);
    
    private final static String JOIN_BOLT = "JoinBolt";
    
    private final static String BASE_CUBOID_BOLT = "BaseCuboidBolt";
    private final static String CUBOID_BOLT = "CuboidBolt";
    
    public StreamCubingJobBuilder() {
        
    }
    
    public void build() {
        
        final TopologyBuilder builder = new TopologyBuilder();
        
        // Phase 1: Create Flat Table with Lookup Tables
        // TODO with json join
        
        // builder.setBolt(JOIN_BPLT, );
    
        // Phase 2: Build Dictionary
        // TODO build index dictionary
        
        // Phase 3: Build Stream Cube
        addLayerCubingBolts(builder);
    
        // Phase 4: Update Info
        
        
    }
    
    private BaseBasicBolt createBaseCuboidBolt() {
        return new BaseCuboidBolt();
    }
    
    private void addLayerCubingBolts(TopologyBuilder builder) {
        String cubeName = null;
        KylinConfig config = AbstractStormJob.loadKylinPropsAndMetadata();
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        RowKeyDesc rowKeyDesc = cubeDesc.getRowkey();
        final int groupRowkeyColumnsCount = cubeDesc.getBuildLevel();
        final int totalRowkeyColumnsCount = rowKeyDesc.getRowKeyColumns().length;
    
        // base cuboid step
        builder.setBolt(BASE_CUBOID_BOLT, createBaseCuboidBolt());
        // n dim cuboid steps
        for (int i = 1; i <= groupRowkeyColumnsCount; i++) {
            int dimNum = totalRowkeyColumnsCount - 1;
            String ndBoltName = String.valueOf(dimNum) + "-Dimension_" + CUBOID_BOLT;
            builder.setBolt(ndBoltName, createNDimensionCuboidBolt(dimNum, totalRowkeyColumnsCount));
        }
    }
    
    private NDCuboidBolt createNDimensionCuboidBolt(int dimNum, int totalRowkeyColumnsCount) {
        
        return new NDCuboidBolt();
    }
}
