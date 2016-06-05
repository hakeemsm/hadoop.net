using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Chain
{
	/// <summary>
	/// The Chain class provides all the common functionality for the
	/// <see cref="ChainMapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// and the
	/// <see cref="ChainReducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// classes.
	/// </summary>
	public class Chain
	{
		protected internal const string ChainMapper = "mapreduce.chain.mapper";

		protected internal const string ChainReducer = "mapreduce.chain.reducer";

		protected internal const string ChainMapperSize = ".size";

		protected internal const string ChainMapperClass = ".mapper.class.";

		protected internal const string ChainMapperConfig = ".mapper.config.";

		protected internal const string ChainReducerClass = ".reducer.class";

		protected internal const string ChainReducerConfig = ".reducer.config";

		protected internal const string MapperInputKeyClass = "mapreduce.chain.mapper.input.key.class";

		protected internal const string MapperInputValueClass = "mapreduce.chain.mapper.input.value.class";

		protected internal const string MapperOutputKeyClass = "mapreduce.chain.mapper.output.key.class";

		protected internal const string MapperOutputValueClass = "mapreduce.chain.mapper.output.value.class";

		protected internal const string ReducerInputKeyClass = "mapreduce.chain.reducer.input.key.class";

		protected internal const string ReducerInputValueClass = "maperduce.chain.reducer.input.value.class";

		protected internal const string ReducerOutputKeyClass = "mapreduce.chain.reducer.output.key.class";

		protected internal const string ReducerOutputValueClass = "mapreduce.chain.reducer.output.value.class";

		protected internal bool isMap;

		private IList<Mapper> mappers = new AList<Mapper>();

		private Reducer<object, object, object, object> reducer;

		private IList<Configuration> confList = new AList<Configuration>();

		private Configuration rConf;

		private IList<Sharpen.Thread> threads = new AList<Sharpen.Thread>();

		private IList<Chain.ChainBlockingQueue<object>> blockingQueues = new AList<Chain.ChainBlockingQueue
			<object>>();

		private Exception throwable = null;

		/// <summary>Creates a Chain instance configured for a Mapper or a Reducer.</summary>
		/// <param name="isMap">
		/// TRUE indicates the chain is for a Mapper, FALSE that is for a
		/// Reducer.
		/// </param>
		protected internal Chain(bool isMap)
		{
			this.isMap = isMap;
		}

		internal class KeyValuePair<K, V>
		{
			internal K key;

			internal V value;

			internal bool endOfInput;

			internal KeyValuePair(K key, V value)
			{
				this.key = key;
				this.value = value;
				this.endOfInput = false;
			}

			internal KeyValuePair(bool eof)
			{
				this.key = null;
				this.value = null;
				this.endOfInput = eof;
			}
		}

		private class ChainRecordReader<Keyin, Valuein> : RecordReader<KEYIN, VALUEIN>
		{
			private Type keyClass;

			private Type valueClass;

			private KEYIN key;

			private VALUEIN value;

			private Configuration conf;

			internal TaskInputOutputContext<KEYIN, VALUEIN, object, object> inputContext = null;

			internal Chain.ChainBlockingQueue<Chain.KeyValuePair<KEYIN, VALUEIN>> inputQueue = 
				null;

			internal ChainRecordReader(Type keyClass, Type valueClass, Chain.ChainBlockingQueue
				<Chain.KeyValuePair<KEYIN, VALUEIN>> inputQueue, Configuration conf)
			{
				// ChainRecordReader either reads from blocking queue or task context.
				// constructor to read from a blocking queue
				this.keyClass = keyClass;
				this.valueClass = valueClass;
				this.inputQueue = inputQueue;
				this.conf = conf;
			}

			internal ChainRecordReader(TaskInputOutputContext<KEYIN, VALUEIN, object, object>
				 context)
			{
				// constructor to read from the context
				inputContext = context;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
			}

			/// <summary>Advance to the next key, value pair, returning null if at end.</summary>
			/// <returns>the key object that was read into, or null if no more</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				if (inputQueue != null)
				{
					return ReadFromQueue();
				}
				else
				{
					if (inputContext.NextKeyValue())
					{
						this.key = inputContext.GetCurrentKey();
						this.value = inputContext.GetCurrentValue();
						return true;
					}
					else
					{
						return false;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private bool ReadFromQueue()
			{
				Chain.KeyValuePair<KEYIN, VALUEIN> kv = null;
				// wait for input on queue
				kv = inputQueue.Dequeue();
				if (kv.endOfInput)
				{
					return false;
				}
				key = (KEYIN)ReflectionUtils.NewInstance(keyClass, conf);
				value = (VALUEIN)ReflectionUtils.NewInstance(valueClass, conf);
				ReflectionUtils.Copy(conf, kv.key, this.key);
				ReflectionUtils.Copy(conf, kv.value, this.value);
				return true;
			}

			/// <summary>Get the current key.</summary>
			/// <returns>the current key object or null if there isn't one</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override KEYIN GetCurrentKey()
			{
				return this.key;
			}

			/// <summary>Get the current value.</summary>
			/// <returns>the value object that was read into</returns>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override VALUEIN GetCurrentValue()
			{
				return this.value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override float GetProgress()
			{
				return 0;
			}
		}

		private class ChainRecordWriter<Keyout, Valueout> : RecordWriter<KEYOUT, VALUEOUT
			>
		{
			internal TaskInputOutputContext<object, object, KEYOUT, VALUEOUT> outputContext = 
				null;

			internal Chain.ChainBlockingQueue<Chain.KeyValuePair<KEYOUT, VALUEOUT>> outputQueue
				 = null;

			internal KEYOUT keyout;

			internal VALUEOUT valueout;

			internal Configuration conf;

			internal Type keyClass;

			internal Type valueClass;

			internal ChainRecordWriter(TaskInputOutputContext<object, object, KEYOUT, VALUEOUT
				> context)
			{
				// ChainRecordWriter either writes to blocking queue or task context
				// constructor to write to context
				outputContext = context;
			}

			internal ChainRecordWriter(Type keyClass, Type valueClass, Chain.ChainBlockingQueue
				<Chain.KeyValuePair<KEYOUT, VALUEOUT>> output, Configuration conf)
			{
				// constructor to write to blocking queue
				this.keyClass = keyClass;
				this.valueClass = valueClass;
				this.outputQueue = output;
				this.conf = conf;
			}

			/// <summary>Writes a key/value pair.</summary>
			/// <param name="key">the key to write.</param>
			/// <param name="value">the value to write.</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(KEYOUT key, VALUEOUT value)
			{
				if (outputQueue != null)
				{
					WriteToQueue(key, value);
				}
				else
				{
					outputContext.Write(key, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void WriteToQueue(KEYOUT key, VALUEOUT value)
			{
				this.keyout = (KEYOUT)ReflectionUtils.NewInstance(keyClass, conf);
				this.valueout = (VALUEOUT)ReflectionUtils.NewInstance(valueClass, conf);
				ReflectionUtils.Copy(conf, key, this.keyout);
				ReflectionUtils.Copy(conf, value, this.valueout);
				// wait to write output to queuue
				outputQueue.Enqueue(new Chain.KeyValuePair<KEYOUT, VALUEOUT>(keyout, valueout));
			}

			/// <summary>Close this <code>RecordWriter</code> to future operations.</summary>
			/// <param name="context">the context of the task</param>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				if (outputQueue != null)
				{
					// write end of input
					outputQueue.Enqueue(new Chain.KeyValuePair<KEYOUT, VALUEOUT>(true));
				}
			}
		}

		private Exception GetThrowable()
		{
			lock (this)
			{
				return throwable;
			}
		}

		private bool SetIfUnsetThrowable(Exception th)
		{
			lock (this)
			{
				if (throwable == null)
				{
					throwable = th;
					return true;
				}
				return false;
			}
		}

		private class MapRunner<Keyin, Valuein, Keyout, Valueout> : Sharpen.Thread
		{
			private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;

			private Mapper.Context chainContext;

			private RecordReader<KEYIN, VALUEIN> rr;

			private RecordWriter<KEYOUT, VALUEOUT> rw;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public MapRunner(Chain _enclosing, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper
				, Mapper.Context mapperContext, RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT
				, VALUEOUT> rw)
			{
				this._enclosing = _enclosing;
				this.mapper = mapper;
				this.rr = rr;
				this.rw = rw;
				this.chainContext = mapperContext;
			}

			public override void Run()
			{
				if (this._enclosing.GetThrowable() != null)
				{
					return;
				}
				try
				{
					this.mapper.Run(this.chainContext);
					this.rr.Close();
					this.rw.Close(this.chainContext);
				}
				catch (Exception th)
				{
					if (this._enclosing.SetIfUnsetThrowable(th))
					{
						this._enclosing.InterruptAllThreads();
					}
				}
			}

			private readonly Chain _enclosing;
		}

		private class ReduceRunner<Keyin, Valuein, Keyout, Valueout> : Sharpen.Thread
		{
			private Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer;

			private Reducer.Context chainContext;

			private RecordWriter<KEYOUT, VALUEOUT> rw;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			internal ReduceRunner(Chain _enclosing, Reducer.Context context, Reducer<KEYIN, VALUEIN
				, KEYOUT, VALUEOUT> reducer, RecordWriter<KEYOUT, VALUEOUT> rw)
			{
				this._enclosing = _enclosing;
				this.reducer = reducer;
				this.chainContext = context;
				this.rw = rw;
			}

			public override void Run()
			{
				try
				{
					this.reducer.Run(this.chainContext);
					this.rw.Close(this.chainContext);
				}
				catch (Exception th)
				{
					if (this._enclosing.SetIfUnsetThrowable(th))
					{
						this._enclosing.InterruptAllThreads();
					}
				}
			}

			private readonly Chain _enclosing;
		}

		internal virtual Configuration GetConf(int index)
		{
			return confList[index];
		}

		/// <summary>
		/// Create a map context that is based on ChainMapContext and the given record
		/// reader and record writer
		/// </summary>
		private Mapper.Context CreateMapContext<Keyin, Valuein, Keyout, Valueout>(RecordReader
			<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw, TaskInputOutputContext<KEYIN
			, VALUEIN, KEYOUT, VALUEOUT> context, Configuration conf)
		{
			MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext = new ChainMapContextImpl
				<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(context, rr, rw, conf);
			Mapper.Context mapperContext = new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT
				>().GetMapContext(mapContext);
			return mapperContext;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void RunMapper(TaskInputOutputContext context, int index)
		{
			Mapper mapper = mappers[index];
			RecordReader rr = new Chain.ChainRecordReader(context);
			RecordWriter rw = new Chain.ChainRecordWriter(context);
			Mapper.Context mapperContext = CreateMapContext(rr, rw, context, GetConf(index));
			mapper.Run(mapperContext);
			rr.Close();
			rw.Close(context);
		}

		/// <summary>
		/// Add mapper(the first mapper) that reads input from the input
		/// context and writes to queue
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void AddMapper(TaskInputOutputContext inputContext, Chain.ChainBlockingQueue
			<Chain.KeyValuePair<object, object>> output, int index)
		{
			Configuration conf = GetConf(index);
			Type keyOutClass = conf.GetClass(MapperOutputKeyClass, typeof(object));
			Type valueOutClass = conf.GetClass(MapperOutputValueClass, typeof(object));
			RecordReader rr = new Chain.ChainRecordReader(inputContext);
			RecordWriter rw = new Chain.ChainRecordWriter(keyOutClass, valueOutClass, output, 
				conf);
			Mapper.Context mapperContext = CreateMapContext(rr, rw, (MapContext)inputContext, 
				GetConf(index));
			Chain.MapRunner runner = new Chain.MapRunner(this, mappers[index], mapperContext, 
				rr, rw);
			threads.AddItem(runner);
		}

		/// <summary>
		/// Add mapper(the last mapper) that reads input from
		/// queue and writes output to the output context
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void AddMapper(Chain.ChainBlockingQueue<Chain.KeyValuePair<object
			, object>> input, TaskInputOutputContext outputContext, int index)
		{
			Configuration conf = GetConf(index);
			Type keyClass = conf.GetClass(MapperInputKeyClass, typeof(object));
			Type valueClass = conf.GetClass(MapperInputValueClass, typeof(object));
			RecordReader rr = new Chain.ChainRecordReader(keyClass, valueClass, input, conf);
			RecordWriter rw = new Chain.ChainRecordWriter(outputContext);
			Chain.MapRunner runner = new Chain.MapRunner(this, mappers[index], CreateMapContext
				(rr, rw, outputContext, GetConf(index)), rr, rw);
			threads.AddItem(runner);
		}

		/// <summary>Add mapper that reads and writes from/to the queue</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void AddMapper(Chain.ChainBlockingQueue<Chain.KeyValuePair<object
			, object>> input, Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>> output
			, TaskInputOutputContext context, int index)
		{
			Configuration conf = GetConf(index);
			Type keyClass = conf.GetClass(MapperInputKeyClass, typeof(object));
			Type valueClass = conf.GetClass(MapperInputValueClass, typeof(object));
			Type keyOutClass = conf.GetClass(MapperOutputKeyClass, typeof(object));
			Type valueOutClass = conf.GetClass(MapperOutputValueClass, typeof(object));
			RecordReader rr = new Chain.ChainRecordReader(keyClass, valueClass, input, conf);
			RecordWriter rw = new Chain.ChainRecordWriter(keyOutClass, valueOutClass, output, 
				conf);
			Chain.MapRunner runner = new Chain.MapRunner(this, mappers[index], CreateMapContext
				(rr, rw, context, GetConf(index)), rr, rw);
			threads.AddItem(runner);
		}

		/// <summary>
		/// Create a reduce context that is based on ChainMapContext and the given
		/// record writer
		/// </summary>
		private Reducer.Context CreateReduceContext<Keyin, Valuein, Keyout, Valueout>(RecordWriter
			<KEYOUT, VALUEOUT> rw, ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context, 
			Configuration conf)
		{
			ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext = new ChainReduceContextImpl
				<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(context, rw, conf);
			Reducer.Context reducerContext = new WrappedReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT
				>().GetReducerContext(reduceContext);
			return reducerContext;
		}

		// Run the reducer directly.
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void RunReducer<Keyin, Valuein, Keyout, Valueout>(TaskInputOutputContext
			<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context)
		{
			RecordWriter<KEYOUT, VALUEOUT> rw = new Chain.ChainRecordWriter<KEYOUT, VALUEOUT>
				(context);
			Reducer.Context reducerContext = CreateReduceContext(rw, (ReduceContext)context, 
				rConf);
			reducer.Run(reducerContext);
			rw.Close(context);
		}

		/// <summary>Add reducer that reads from context and writes to a queue</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void AddReducer(TaskInputOutputContext inputContext, Chain.ChainBlockingQueue
			<Chain.KeyValuePair<object, object>> outputQueue)
		{
			Type keyOutClass = rConf.GetClass(ReducerOutputKeyClass, typeof(object));
			Type valueOutClass = rConf.GetClass(ReducerOutputValueClass, typeof(object));
			RecordWriter rw = new Chain.ChainRecordWriter(keyOutClass, valueOutClass, outputQueue
				, rConf);
			Reducer.Context reducerContext = CreateReduceContext(rw, (ReduceContext)inputContext
				, rConf);
			Chain.ReduceRunner runner = new Chain.ReduceRunner(this, reducerContext, reducer, 
				rw);
			threads.AddItem(runner);
		}

		// start all the threads
		internal virtual void StartAllThreads()
		{
			foreach (Sharpen.Thread thread in threads)
			{
				thread.Start();
			}
		}

		// wait till all threads finish
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void JoinAllThreads()
		{
			foreach (Sharpen.Thread thread in threads)
			{
				thread.Join();
			}
			Exception th = GetThrowable();
			if (th != null)
			{
				if (th is IOException)
				{
					throw (IOException)th;
				}
				else
				{
					if (th is Exception)
					{
						throw (Exception)th;
					}
					else
					{
						throw new RuntimeException(th);
					}
				}
			}
		}

		// interrupt all threads
		private void InterruptAllThreads()
		{
			lock (this)
			{
				foreach (Sharpen.Thread th in threads)
				{
					th.Interrupt();
				}
				foreach (Chain.ChainBlockingQueue<object> queue in blockingQueues)
				{
					queue.Interrupt();
				}
			}
		}

		/// <summary>
		/// Returns the prefix to use for the configuration of the chain depending if
		/// it is for a Mapper or a Reducer.
		/// </summary>
		/// <param name="isMap">TRUE for Mapper, FALSE for Reducer.</param>
		/// <returns>the prefix to use.</returns>
		protected internal static string GetPrefix(bool isMap)
		{
			return (isMap) ? ChainMapper : ChainReducer;
		}

		protected internal static int GetIndex(Configuration conf, string prefix)
		{
			return conf.GetInt(prefix + ChainMapperSize, 0);
		}

		/// <summary>
		/// Creates a
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// for the Map or Reduce in the chain.
		/// <p>
		/// It creates a new Configuration using the chain job's Configuration as base
		/// and adds to it the configuration properties for the chain element. The keys
		/// of the chain element Configuration have precedence over the given
		/// Configuration.
		/// </p>
		/// </summary>
		/// <param name="jobConf">the chain job's Configuration.</param>
		/// <param name="confKey">
		/// the key for chain element configuration serialized in the chain
		/// job's Configuration.
		/// </param>
		/// <returns>
		/// a new Configuration aggregating the chain job's Configuration with
		/// the chain element configuration properties.
		/// </returns>
		protected internal static Configuration GetChainElementConf(Configuration jobConf
			, string confKey)
		{
			Configuration conf = null;
			try
			{
				using (Stringifier<Configuration> stringifier = new DefaultStringifier<Configuration
					>(jobConf, typeof(Configuration)))
				{
					string confString = jobConf.Get(confKey, null);
					if (confString != null)
					{
						conf = stringifier.FromString(jobConf.Get(confKey, null));
					}
				}
			}
			catch (IOException ioex)
			{
				throw new RuntimeException(ioex);
			}
			// we have to do this because the Writable desearialization clears all
			// values set in the conf making not possible do a
			// new Configuration(jobConf) in the creation of the conf above
			jobConf = new Configuration(jobConf);
			if (conf != null)
			{
				foreach (KeyValuePair<string, string> entry in conf)
				{
					jobConf.Set(entry.Key, entry.Value);
				}
			}
			return jobConf;
		}

		/// <summary>Adds a Mapper class to the chain job.</summary>
		/// <remarks>
		/// Adds a Mapper class to the chain job.
		/// <p>
		/// The configuration properties of the chain job have precedence over the
		/// configuration properties of the Mapper.
		/// </remarks>
		/// <param name="isMap">indicates if the Chain is for a Mapper or for a Reducer.</param>
		/// <param name="job">chain job.</param>
		/// <param name="klass">the Mapper class to add.</param>
		/// <param name="inputKeyClass">mapper input key class.</param>
		/// <param name="inputValueClass">mapper input value class.</param>
		/// <param name="outputKeyClass">mapper output key class.</param>
		/// <param name="outputValueClass">mapper output value class.</param>
		/// <param name="mapperConf">
		/// a configuration for the Mapper class. It is recommended to use a
		/// Configuration without default values using the
		/// <code>Configuration(boolean loadDefaults)</code> constructor with
		/// FALSE.
		/// </param>
		protected internal static void AddMapper(bool isMap, Job job, Type klass, Type inputKeyClass
			, Type inputValueClass, Type outputKeyClass, Type outputValueClass, Configuration
			 mapperConf)
		{
			string prefix = GetPrefix(isMap);
			Configuration jobConf = job.GetConfiguration();
			// if a reducer chain check the Reducer has been already set
			CheckReducerAlreadySet(isMap, jobConf, prefix, true);
			// set the mapper class
			int index = GetIndex(jobConf, prefix);
			jobConf.SetClass(prefix + ChainMapperClass + index, klass, typeof(Mapper));
			ValidateKeyValueTypes(isMap, jobConf, inputKeyClass, inputValueClass, outputKeyClass
				, outputValueClass, index, prefix);
			SetMapperConf(isMap, jobConf, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass
				, mapperConf, index, prefix);
		}

		// if a reducer chain check the Reducer has been already set or not
		protected internal static void CheckReducerAlreadySet(bool isMap, Configuration jobConf
			, string prefix, bool shouldSet)
		{
			if (!isMap)
			{
				if (shouldSet)
				{
					if (jobConf.GetClass(prefix + ChainReducerClass, null) == null)
					{
						throw new InvalidOperationException("A Mapper can be added to the chain only after the Reducer has "
							 + "been set");
					}
				}
				else
				{
					if (jobConf.GetClass(prefix + ChainReducerClass, null) != null)
					{
						throw new InvalidOperationException("Reducer has been already set");
					}
				}
			}
		}

		protected internal static void ValidateKeyValueTypes(bool isMap, Configuration jobConf
			, Type inputKeyClass, Type inputValueClass, Type outputKeyClass, Type outputValueClass
			, int index, string prefix)
		{
			// if it is a reducer chain and the first Mapper is being added check the
			// key and value input classes of the mapper match those of the reducer
			// output.
			if (!isMap && index == 0)
			{
				Configuration reducerConf = GetChainElementConf(jobConf, prefix + ChainReducerConfig
					);
				if (!inputKeyClass.IsAssignableFrom(reducerConf.GetClass(ReducerOutputKeyClass, null
					)))
				{
					throw new ArgumentException("The Reducer output key class does" + " not match the Mapper input key class"
						);
				}
				if (!inputValueClass.IsAssignableFrom(reducerConf.GetClass(ReducerOutputValueClass
					, null)))
				{
					throw new ArgumentException("The Reducer output value class" + " does not match the Mapper input value class"
						);
				}
			}
			else
			{
				if (index > 0)
				{
					// check the that the new Mapper in the chain key and value input classes
					// match those of the previous Mapper output.
					Configuration previousMapperConf = GetChainElementConf(jobConf, prefix + ChainMapperConfig
						 + (index - 1));
					if (!inputKeyClass.IsAssignableFrom(previousMapperConf.GetClass(MapperOutputKeyClass
						, null)))
					{
						throw new ArgumentException("The specified Mapper input key class does" + " not match the previous Mapper's output key class."
							);
					}
					if (!inputValueClass.IsAssignableFrom(previousMapperConf.GetClass(MapperOutputValueClass
						, null)))
					{
						throw new ArgumentException("The specified Mapper input value class" + " does not match the previous Mapper's output value class."
							);
					}
				}
			}
		}

		protected internal static void SetMapperConf(bool isMap, Configuration jobConf, Type
			 inputKeyClass, Type inputValueClass, Type outputKeyClass, Type outputValueClass
			, Configuration mapperConf, int index, string prefix)
		{
			// if the Mapper does not have a configuration, create an empty one
			if (mapperConf == null)
			{
				// using a Configuration without defaults to make it lightweight.
				// still the chain's conf may have all defaults and this conf is
				// overlapped to the chain configuration one.
				mapperConf = new Configuration(true);
			}
			// store the input/output classes of the mapper in the mapper conf
			mapperConf.SetClass(MapperInputKeyClass, inputKeyClass, typeof(object));
			mapperConf.SetClass(MapperInputValueClass, inputValueClass, typeof(object));
			mapperConf.SetClass(MapperOutputKeyClass, outputKeyClass, typeof(object));
			mapperConf.SetClass(MapperOutputValueClass, outputValueClass, typeof(object));
			// serialize the mapper configuration in the chain configuration.
			Stringifier<Configuration> stringifier = new DefaultStringifier<Configuration>(jobConf
				, typeof(Configuration));
			try
			{
				jobConf.Set(prefix + ChainMapperConfig + index, stringifier.ToString(new Configuration
					(mapperConf)));
			}
			catch (IOException ioEx)
			{
				throw new RuntimeException(ioEx);
			}
			// increment the chain counter
			jobConf.SetInt(prefix + ChainMapperSize, index + 1);
		}

		/// <summary>Sets the Reducer class to the chain job.</summary>
		/// <remarks>
		/// Sets the Reducer class to the chain job.
		/// <p>
		/// The configuration properties of the chain job have precedence over the
		/// configuration properties of the Reducer.
		/// </remarks>
		/// <param name="job">the chain job.</param>
		/// <param name="klass">the Reducer class to add.</param>
		/// <param name="inputKeyClass">reducer input key class.</param>
		/// <param name="inputValueClass">reducer input value class.</param>
		/// <param name="outputKeyClass">reducer output key class.</param>
		/// <param name="outputValueClass">reducer output value class.</param>
		/// <param name="reducerConf">
		/// a configuration for the Reducer class. It is recommended to use a
		/// Configuration without default values using the
		/// <code>Configuration(boolean loadDefaults)</code> constructor with
		/// FALSE.
		/// </param>
		protected internal static void SetReducer(Job job, Type klass, Type inputKeyClass
			, Type inputValueClass, Type outputKeyClass, Type outputValueClass, Configuration
			 reducerConf)
		{
			string prefix = GetPrefix(false);
			Configuration jobConf = job.GetConfiguration();
			CheckReducerAlreadySet(false, jobConf, prefix, false);
			jobConf.SetClass(prefix + ChainReducerClass, klass, typeof(Reducer));
			SetReducerConf(jobConf, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass
				, reducerConf, prefix);
		}

		protected internal static void SetReducerConf(Configuration jobConf, Type inputKeyClass
			, Type inputValueClass, Type outputKeyClass, Type outputValueClass, Configuration
			 reducerConf, string prefix)
		{
			// if the Reducer does not have a Configuration, create an empty one
			if (reducerConf == null)
			{
				// using a Configuration without defaults to make it lightweight.
				// still the chain's conf may have all defaults and this conf is
				// overlapped to the chain's Configuration one.
				reducerConf = new Configuration(false);
			}
			// store the input/output classes of the reducer in
			// the reducer configuration
			reducerConf.SetClass(ReducerInputKeyClass, inputKeyClass, typeof(object));
			reducerConf.SetClass(ReducerInputValueClass, inputValueClass, typeof(object));
			reducerConf.SetClass(ReducerOutputKeyClass, outputKeyClass, typeof(object));
			reducerConf.SetClass(ReducerOutputValueClass, outputValueClass, typeof(object));
			// serialize the reducer configuration in the chain's configuration.
			Stringifier<Configuration> stringifier = new DefaultStringifier<Configuration>(jobConf
				, typeof(Configuration));
			try
			{
				jobConf.Set(prefix + ChainReducerConfig, stringifier.ToString(new Configuration(reducerConf
					)));
			}
			catch (IOException ioEx)
			{
				throw new RuntimeException(ioEx);
			}
		}

		/// <summary>Setup the chain.</summary>
		/// <param name="jobConf">
		/// chain job's
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// .
		/// </param>
		internal virtual void Setup(Configuration jobConf)
		{
			string prefix = GetPrefix(isMap);
			int index = jobConf.GetInt(prefix + ChainMapperSize, 0);
			for (int i = 0; i < index; i++)
			{
				Type klass = jobConf.GetClass<Mapper>(prefix + ChainMapperClass + i, null);
				Configuration mConf = GetChainElementConf(jobConf, prefix + ChainMapperConfig + i
					);
				confList.AddItem(mConf);
				Mapper mapper = ReflectionUtils.NewInstance(klass, mConf);
				mappers.AddItem(mapper);
			}
			Type klass_1 = jobConf.GetClass<Reducer>(prefix + ChainReducerClass, null);
			if (klass_1 != null)
			{
				rConf = GetChainElementConf(jobConf, prefix + ChainReducerConfig);
				reducer = ReflectionUtils.NewInstance(klass_1, rConf);
			}
		}

		internal virtual IList<Mapper> GetAllMappers()
		{
			return mappers;
		}

		/// <summary>Returns the Reducer instance in the chain.</summary>
		/// <returns>the Reducer instance in the chain or NULL if none.</returns>
		internal virtual Reducer<object, object, object, object> GetReducer()
		{
			return reducer;
		}

		/// <summary>Creates a ChainBlockingQueue with KeyValuePair as element</summary>
		/// <returns>the ChainBlockingQueue</returns>
		internal virtual Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>> CreateBlockingQueue
			()
		{
			return new Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>>(this);
		}

		/// <summary>A blocking queue with one element.</summary>
		/// <?/>
		internal class ChainBlockingQueue<E>
		{
			internal E element = null;

			internal bool isInterrupted = false;

			internal ChainBlockingQueue(Chain _enclosing)
			{
				this._enclosing = _enclosing;
				this._enclosing.blockingQueues.AddItem(this);
			}

			/// <exception cref="System.Exception"/>
			internal virtual void Enqueue(E e)
			{
				lock (this)
				{
					while (this.element != null)
					{
						if (this.isInterrupted)
						{
							throw new Exception();
						}
						Sharpen.Runtime.Wait(this);
					}
					this.element = e;
					Sharpen.Runtime.Notify(this);
				}
			}

			/// <exception cref="System.Exception"/>
			internal virtual E Dequeue()
			{
				lock (this)
				{
					while (this.element == null)
					{
						if (this.isInterrupted)
						{
							throw new Exception();
						}
						Sharpen.Runtime.Wait(this);
					}
					E e = this.element;
					this.element = null;
					Sharpen.Runtime.Notify(this);
					return e;
				}
			}

			internal virtual void Interrupt()
			{
				lock (this)
				{
					this.isInterrupted = true;
					Sharpen.Runtime.NotifyAll(this);
				}
			}

			private readonly Chain _enclosing;
		}
	}
}
