package org.apache.kylin.engine.storm.StepBolts;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.model.CubeDesc;

import org.apache.kylin.engine.storm.common.AbstractStormJob;
import org.apache.kylin.engine.storm.common.StreamConstants;

import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by copperfield on 22/01/2017.
 */


public class CuboidBoltBase extends BaseBasicBolt {
    
    private final static Logger logger = LoggerFactory.getLogger(CuboidBoltBase.class);
    public static final byte[] STORM_NULL = Bytes.toBytes("\\N");      // to be changed for storm
    protected String cubeName;
    protected Cuboid baseCuboid;
    protected CubeInstance cube;
    protected CubeDesc cubeDesc;
    protected List<byte[]> nullBytes;
    protected int counter;
    protected MeasureIngester<?>[] aggrIngesters;
    protected Object[] measures;
    protected byte[][] keyBytesBuf;
    protected BytesSplitter bytesSplitter;
    protected AbstractRowKeyEncoder rowKeyEncoder;
    protected BufferedMeasureCodec measureCodec;
    private int errorRecordCounter;
    protected byte[] outputKey;
    protected List<byte[]> outputValue;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        
        cubeName = null;    // Think about how to get cube name
                
        KylinConfig config = AbstractStormJob.loadKylinPropsAndMetadata();
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        
        bytesSplitter = new BytesSplitter(200, 16384);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(null, baseCuboid);   // Stream message don't need segment?
        
        measureCodec = new BufferedMeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];
        
        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];
        
        aggrIngesters = MeasureIngester.create(cube.getMeasures());
        
        initNullBytes();
    }
    
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // outputFieldsDeclarer.declare(new Fields(outputKey, outputValue));   // need to modify
    }
    
    private void initNullBytes() {
        nullBytes = Lists.newArrayList();
        nullBytes.add(STORM_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }
    
    protected boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }
    
    protected byte[] buildKey(SplittedBytes[] splitBuffers) {
        int[] rowKeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();  // the table should be design again
        for (int i = 0; i < baseCuboid.getColumns().size(); i++) {
            int index = rowKeyColumnIndexes[i];
            keyBytesBuf[i] = Arrays.copyOf(splitBuffers[index].value, splitBuffers[index].length);
            if (isNull(keyBytesBuf[i])) {
                keyBytesBuf[i] = null;
            }
        }
        return rowKeyEncoder.encode(keyBytesBuf);
    }
    
    private ByteBuffer buildValue(SplittedBytes[] splitBuffers) {
        
        for (int i = 0; i < measures.length; i++) {
            measures[i] = buildValueOf(i, splitBuffers);
        }
        
        return measureCodec.encode(measures);
    }
    
    private Object buildValueOf(int idxOfMeasure, SplittedBytes[] splitBuffers) {
        MeasureDesc measure = cubeDesc.getMeasures().get(idxOfMeasure);
        FunctionDesc function = measure.getFunction();
        int[] colIdxOnFlatTable = intermediateTableDesc.getMeasureColumnIndexes()[idxOfMeasure];  // the table should be design again
        
        int paramCount = function.getParameterCount();
        String[] inputToMeasure = new String[paramCount];
        
        // pick up parameter values
        ParameterDesc param = function.getParameter();
        int colParamIdx = 0; // index among parameters of column type
        for (int i = 0; i < paramCount; i++, param = param.getNextParameter()) {
            String value;
            if (function.isCount()) {
                value = "1";
            } else if (param.isColumnType()) {
                value = getCell(colIdxOnFlatTable[colParamIdx++], splitBuffers);
            } else {
                value = param.getValue();
            }
            inputToMeasure[i] = value;
        }
        
        return aggrIngesters[idxOfMeasure].valueOf(inputToMeasure, measure, null);   // no consider dictionary
    }
    
    private String getCell(int i, SplittedBytes[] splitBuffers) {
        byte[] bytes = Arrays.copyOf(splitBuffers[i].value, splitBuffers[i].length);
        if (isNull(bytes))
            return null;
        else
            return Bytes.toString(bytes);
    }
    
    protected void outputKV(BasicOutputCollector basicOutputCollector) {
        List<byte[]> output = new ArrayList<byte[]>();
        outputKey = buildKey(bytesSplitter.getSplitBuffers());
    
        ByteBuffer valueBuf = buildValue(bytesSplitter.getSplitBuffers());
        outputValue.addAll()
        
        output.addAll(outputValue);
        basicOutputCollector.emit(new Values(output));    // should be check!
    }
    
    protected byte[][] convertUTF8Bytes(String[] row) throws UnsupportedEncodingException {
        byte[][] result = new byte[row.length][];
        for (int i = 0; i < row.length; i++) {
            result[i] = row[i] == null ? STORM_NULL : row[i].getBytes("UTF-8");
        }
        return result;
    }
    
    protected void handleErrorRecord(Exception ex) {
        
        // logger.error("Insane record: " + bytesSplitter, ex);
        
        // TODO expose errorRecordCounter as hadoop counter
        errorRecordCounter++;
        if (errorRecordCounter > StreamConstants.ERROR_RECORD_LOG_THRESHOLD) {
            if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException("", ex);
        }
    }
}
