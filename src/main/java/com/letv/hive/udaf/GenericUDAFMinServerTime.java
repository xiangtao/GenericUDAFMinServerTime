package com.letv.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * @author taox
 */
public class GenericUDAFMinServerTime extends AbstractGenericUDAFResolver {
	  static final Log LOG = LogFactory.getLog(GenericUDAFMinServerTime.class.getName());
	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
			throws SemanticException {
		if (parameters.length != 2) {
		      throw new UDFArgumentTypeException(parameters.length - 2,
		          "Exactly two argument is expected.");
		}
		ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
		if (!ObjectInspectorUtils.compareSupported(oi)) {
		      throw new UDFArgumentTypeException(parameters.length - 1,
		          "Cannot support comparison of map<> type or complex type containing map<>.");
		}
		return new GenericUDAFMinServerTimeEvaluator();
	}
	
	public static class GenericUDAFMinServerTimeEvaluator extends GenericUDAFEvaluator {

		ObjectInspector inputOI;
		ObjectInspector inputOI2;
	    ObjectInspector outputOI;
	    ObjectInspector outputOI2;
	    
		@Override
	    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException{
			assert (parameters.length == 2);
			super.init(m, parameters);
		    inputOI = parameters[0];
		    inputOI2 = parameters[1];
		    // Copy to Java object because that saves object creation time.
		    // Note that on average the number of copies is log(N) so that's not
		    // very important.
		    outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
			          ObjectInspectorCopyOption.JAVA);
		    outputOI2 = ObjectInspectorUtils.getStandardObjectInspector(inputOI2,
		          ObjectInspectorCopyOption.JAVA);
		    return outputOI;
			
		}
		
		/** class for storing the current max value */
	    static class MinAgg implements AggregationBuffer {
	      Object o;
	      Object out;
	    }
	    
	    
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			 MinAgg result = new MinAgg();
		      return result;
		}

		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			MinAgg myagg = (MinAgg) agg;
		      myagg.o = null;
		      myagg.out = null;
		}

		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			  assert (parameters.length == 2);
			  Object partial = parameters[0];
			  if(partial!=null){
				  MinAgg myagg = (MinAgg) agg;
			        int r = ObjectInspectorUtils.compare(myagg.o, outputOI, partial, inputOI);
			        if (myagg.o == null || r > 0) {
			          myagg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
			              ObjectInspectorCopyOption.JAVA);
			          myagg.out = ObjectInspectorUtils.copyToStandardObject(parameters[1], inputOI2,
				              ObjectInspectorCopyOption.JAVA);
			        }
			  }
		}

		@Override
		public Object terminatePartial(AggregationBuffer agg)
				throws HiveException {
			return null;
		}

		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			if (partial != null) {
		        MinAgg myagg = (MinAgg) agg;
		        int r = ObjectInspectorUtils.compare(myagg.o, outputOI, partial, inputOI);
		        if (myagg.o == null || r > 0) {
		          myagg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
		              ObjectInspectorCopyOption.JAVA);
		        }
		      }
		}

		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			return null;
		}
		
		
		
	}
}
