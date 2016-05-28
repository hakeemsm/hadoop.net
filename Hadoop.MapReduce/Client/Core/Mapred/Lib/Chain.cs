using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// The Chain class provides all the common functionality for the
	/// <see cref="ChainMapper"/>
	/// and the
	/// <see cref="ChainReducer"/>
	/// classes.
	/// </summary>
	internal class Chain : Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain
	{
		private const string MapperByValue = "chain.mapper.byValue";

		private const string ReducerByValue = "chain.reducer.byValue";

		private JobConf chainJobConf;

		private IList<Mapper> mappers = new AList<Mapper>();

		private Reducer reducer;

		private IList<Serialization> mappersKeySerialization = new AList<Serialization>();

		private IList<Serialization> mappersValueSerialization = new AList<Serialization>
			();

		private Serialization reducerKeySerialization;

		private Serialization reducerValueSerialization;

		/// <summary>Creates a Chain instance configured for a Mapper or a Reducer.</summary>
		/// <param name="isMap">
		/// TRUE indicates the chain is for a Mapper, FALSE that is for a
		/// Reducer.
		/// </param>
		internal Chain(bool isMap)
			: base(isMap)
		{
		}

		// to cache the key/value output class serializations for each chain element
		// to avoid everytime lookup.
		/// <summary>Adds a Mapper class to the chain job's JobConf.</summary>
		/// <remarks>
		/// Adds a Mapper class to the chain job's JobConf.
		/// <p/>
		/// The configuration properties of the chain job have precedence over the
		/// configuration properties of the Mapper.
		/// </remarks>
		/// <param name="isMap">
		/// indicates if the Chain is for a Mapper or for a
		/// Reducer.
		/// </param>
		/// <param name="jobConf">chain job's JobConf to add the Mapper class.</param>
		/// <param name="klass">the Mapper class to add.</param>
		/// <param name="inputKeyClass">mapper input key class.</param>
		/// <param name="inputValueClass">mapper input value class.</param>
		/// <param name="outputKeyClass">mapper output key class.</param>
		/// <param name="outputValueClass">mapper output value class.</param>
		/// <param name="byValue">
		/// indicates if key/values should be passed by value
		/// to the next Mapper in the chain, if any.
		/// </param>
		/// <param name="mapperConf">
		/// a JobConf with the configuration for the Mapper
		/// class. It is recommended to use a JobConf without default values using the
		/// <code>JobConf(boolean loadDefaults)</code> constructor with FALSE.
		/// </param>
		public static void AddMapper<K1, V1, K2, V2>(bool isMap, JobConf jobConf, Type klass
			, Type inputKeyClass, Type inputValueClass, Type outputKeyClass, Type outputValueClass
			, bool byValue, JobConf mapperConf)
		{
			string prefix = GetPrefix(isMap);
			// if a reducer chain check the Reducer has been already set
			CheckReducerAlreadySet(isMap, jobConf, prefix, true);
			// set the mapper class
			int index = GetIndex(jobConf, prefix);
			jobConf.SetClass(prefix + ChainMapperClass + index, klass, typeof(Mapper));
			ValidateKeyValueTypes(isMap, jobConf, inputKeyClass, inputValueClass, outputKeyClass
				, outputValueClass, index, prefix);
			// if the Mapper does not have a private JobConf create an empty one
			if (mapperConf == null)
			{
				// using a JobConf without defaults to make it lightweight.
				// still the chain JobConf may have all defaults and this conf is
				// overlapped to the chain JobConf one.
				mapperConf = new JobConf(true);
			}
			// store in the private mapper conf if it works by value or by reference
			mapperConf.SetBoolean(MapperByValue, byValue);
			SetMapperConf(isMap, jobConf, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass
				, mapperConf, index, prefix);
		}

		/// <summary>Sets the Reducer class to the chain job's JobConf.</summary>
		/// <remarks>
		/// Sets the Reducer class to the chain job's JobConf.
		/// <p/>
		/// The configuration properties of the chain job have precedence over the
		/// configuration properties of the Reducer.
		/// </remarks>
		/// <param name="jobConf">chain job's JobConf to add the Reducer class.</param>
		/// <param name="klass">the Reducer class to add.</param>
		/// <param name="inputKeyClass">reducer input key class.</param>
		/// <param name="inputValueClass">reducer input value class.</param>
		/// <param name="outputKeyClass">reducer output key class.</param>
		/// <param name="outputValueClass">reducer output value class.</param>
		/// <param name="byValue">
		/// indicates if key/values should be passed by value
		/// to the next Mapper in the chain, if any.
		/// </param>
		/// <param name="reducerConf">
		/// a JobConf with the configuration for the Reducer
		/// class. It is recommended to use a JobConf without default values using the
		/// <code>JobConf(boolean loadDefaults)</code> constructor with FALSE.
		/// </param>
		public static void SetReducer<K1, V1, K2, V2>(JobConf jobConf, Type klass, Type inputKeyClass
			, Type inputValueClass, Type outputKeyClass, Type outputValueClass, bool byValue
			, JobConf reducerConf)
		{
			string prefix = GetPrefix(false);
			CheckReducerAlreadySet(false, jobConf, prefix, false);
			jobConf.SetClass(prefix + ChainReducerClass, klass, typeof(Reducer));
			// if the Reducer does not have a private JobConf create an empty one
			if (reducerConf == null)
			{
				// using a JobConf without defaults to make it lightweight.
				// still the chain JobConf may have all defaults and this conf is
				// overlapped to the chain JobConf one.
				reducerConf = new JobConf(false);
			}
			// store in the private reducer conf the input/output classes of the reducer
			// and if it works by value or by reference
			reducerConf.SetBoolean(ReducerByValue, byValue);
			SetReducerConf(jobConf, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass
				, reducerConf, prefix);
		}

		/// <summary>Configures all the chain elements for the task.</summary>
		/// <param name="jobConf">chain job's JobConf.</param>
		public virtual void Configure(JobConf jobConf)
		{
			string prefix = GetPrefix(isMap);
			chainJobConf = jobConf;
			SerializationFactory serializationFactory = new SerializationFactory(chainJobConf
				);
			int index = jobConf.GetInt(prefix + ChainMapperSize, 0);
			for (int i = 0; i < index; i++)
			{
				Type klass = jobConf.GetClass<Mapper>(prefix + ChainMapperClass + i, null);
				JobConf mConf = new JobConf(GetChainElementConf(jobConf, prefix + ChainMapperConfig
					 + i));
				Mapper mapper = ReflectionUtils.NewInstance(klass, mConf);
				mappers.AddItem(mapper);
				if (mConf.GetBoolean(MapperByValue, true))
				{
					mappersKeySerialization.AddItem(serializationFactory.GetSerialization(mConf.GetClass
						(MapperOutputKeyClass, null)));
					mappersValueSerialization.AddItem(serializationFactory.GetSerialization(mConf.GetClass
						(MapperOutputValueClass, null)));
				}
				else
				{
					mappersKeySerialization.AddItem(null);
					mappersValueSerialization.AddItem(null);
				}
			}
			Type klass_1 = jobConf.GetClass<Reducer>(prefix + ChainReducerClass, null);
			if (klass_1 != null)
			{
				JobConf rConf = new JobConf(GetChainElementConf(jobConf, prefix + ChainReducerConfig
					));
				reducer = ReflectionUtils.NewInstance(klass_1, rConf);
				if (rConf.GetBoolean(ReducerByValue, true))
				{
					reducerKeySerialization = serializationFactory.GetSerialization(rConf.GetClass(ReducerOutputKeyClass
						, null));
					reducerValueSerialization = serializationFactory.GetSerialization(rConf.GetClass(
						ReducerOutputValueClass, null));
				}
				else
				{
					reducerKeySerialization = null;
					reducerValueSerialization = null;
				}
			}
		}

		/// <summary>Returns the chain job conf.</summary>
		/// <returns>the chain job conf.</returns>
		protected internal virtual JobConf GetChainJobConf()
		{
			return chainJobConf;
		}

		/// <summary>Returns the first Mapper instance in the chain.</summary>
		/// <returns>the first Mapper instance in the chain or NULL if none.</returns>
		public virtual Mapper GetFirstMap()
		{
			return (mappers.Count > 0) ? mappers[0] : null;
		}

		/// <summary>Returns the Reducer instance in the chain.</summary>
		/// <returns>the Reducer instance in the chain or NULL if none.</returns>
		internal override Reducer<object, object, object, object> GetReducer()
		{
			return reducer;
		}

		/// <summary>Returns the OutputCollector to be used by a Mapper instance in the chain.
		/// 	</summary>
		/// <param name="mapperIndex">index of the Mapper instance to get the OutputCollector.
		/// 	</param>
		/// <param name="output">the original OutputCollector of the task.</param>
		/// <param name="reporter">the reporter of the task.</param>
		/// <returns>the OutputCollector to be used in the chain.</returns>
		public virtual OutputCollector GetMapperCollector(int mapperIndex, OutputCollector
			 output, Reporter reporter)
		{
			Serialization keySerialization = mappersKeySerialization[mapperIndex];
			Serialization valueSerialization = mappersValueSerialization[mapperIndex];
			return new Chain.ChainOutputCollector(this, mapperIndex, keySerialization, valueSerialization
				, output, reporter);
		}

		/// <summary>Returns the OutputCollector to be used by a Mapper instance in the chain.
		/// 	</summary>
		/// <param name="output">the original OutputCollector of the task.</param>
		/// <param name="reporter">the reporter of the task.</param>
		/// <returns>the OutputCollector to be used in the chain.</returns>
		public virtual OutputCollector GetReducerCollector(OutputCollector output, Reporter
			 reporter)
		{
			return new Chain.ChainOutputCollector(this, reducerKeySerialization, reducerValueSerialization
				, output, reporter);
		}

		/// <summary>Closes all the chain elements.</summary>
		/// <exception cref="System.IO.IOException">
		/// thrown if any of the chain elements threw an
		/// IOException exception.
		/// </exception>
		public virtual void Close()
		{
			foreach (Mapper map in mappers)
			{
				map.Close();
			}
			if (reducer != null)
			{
				reducer.Close();
			}
		}

		private sealed class _ThreadLocal_294 : ThreadLocal<DataOutputBuffer>
		{
			public _ThreadLocal_294()
			{
			}

			// using a ThreadLocal to reuse the ByteArrayOutputStream used for ser/deser
			// it has to be a thread local because if not it would break if used from a
			// MultiThreadedMapRunner.
			protected override DataOutputBuffer InitialValue()
			{
				return new DataOutputBuffer(1024);
			}
		}

		private ThreadLocal<DataOutputBuffer> threadLocalDataOutputBuffer = new _ThreadLocal_294
			();

		/// <summary>OutputCollector implementation used by the chain tasks.</summary>
		/// <remarks>
		/// OutputCollector implementation used by the chain tasks.
		/// <p/>
		/// If it is not the end of the chain, a
		/// <see cref="ChainOutputCollector{K, V}.Collect(object, object)"/>
		/// invocation invokes
		/// the next Mapper in the chain. If it is the end of the chain the task
		/// OutputCollector is called.
		/// </remarks>
		private class ChainOutputCollector<K, V> : OutputCollector<K, V>
		{
			private int nextMapperIndex;

			private Serialization<K> keySerialization;

			private Serialization<V> valueSerialization;

			private OutputCollector output;

			private Reporter reporter;

			public ChainOutputCollector(Chain _enclosing, int index, Serialization<K> keySerialization
				, Serialization<V> valueSerialization, OutputCollector output, Reporter reporter
				)
			{
				this._enclosing = _enclosing;
				/*
				* Constructor for Mappers
				*/
				this.nextMapperIndex = index + 1;
				this.keySerialization = keySerialization;
				this.valueSerialization = valueSerialization;
				this.output = output;
				this.reporter = reporter;
			}

			public ChainOutputCollector(Chain _enclosing, Serialization<K> keySerialization, 
				Serialization<V> valueSerialization, OutputCollector output, Reporter reporter)
			{
				this._enclosing = _enclosing;
				/*
				* Constructor for Reducer
				*/
				this.nextMapperIndex = 0;
				this.keySerialization = keySerialization;
				this.valueSerialization = valueSerialization;
				this.output = output;
				this.reporter = reporter;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Collect(K key, V value)
			{
				if (this.nextMapperIndex < this._enclosing.mappers.Count)
				{
					// there is a next mapper in chain
					// only need to ser/deser if there is next mapper in the chain
					if (this.keySerialization != null)
					{
						key = this.MakeCopyForPassByValue(this.keySerialization, key);
						value = this.MakeCopyForPassByValue(this.valueSerialization, value);
					}
					// gets ser/deser and mapper of next in chain
					Serialization nextKeySerialization = this._enclosing.mappersKeySerialization[this
						.nextMapperIndex];
					Serialization nextValueSerialization = this._enclosing.mappersValueSerialization[
						this.nextMapperIndex];
					Mapper nextMapper = this._enclosing.mappers[this.nextMapperIndex];
					// invokes next mapper in chain
					nextMapper.Map(key, value, new Chain.ChainOutputCollector(this, this.nextMapperIndex
						, nextKeySerialization, nextValueSerialization, this.output, this.reporter), this
						.reporter);
				}
				else
				{
					// end of chain, user real output collector
					this.output.Collect(key, value);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private E MakeCopyForPassByValue<E>(Serialization<E> serialization, E obj)
			{
				Org.Apache.Hadoop.IO.Serializer.Serializer<E> ser = serialization.GetSerializer(GenericsUtil
					.GetClass(obj));
				Deserializer<E> deser = serialization.GetDeserializer(GenericsUtil.GetClass(obj));
				DataOutputBuffer dof = this._enclosing.threadLocalDataOutputBuffer.Get();
				dof.Reset();
				ser.Open(dof);
				ser.Serialize(obj);
				ser.Close();
				obj = ReflectionUtils.NewInstance(GenericsUtil.GetClass(obj), this._enclosing.GetChainJobConf
					());
				ByteArrayInputStream bais = new ByteArrayInputStream(dof.GetData(), 0, dof.GetLength
					());
				deser.Open(bais);
				deser.Deserialize(obj);
				deser.Close();
				return obj;
			}

			private readonly Chain _enclosing;
		}
	}
}
