using System;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// The ChainMapper class allows to use multiple Mapper classes within a single
	/// Map task.
	/// </summary>
	/// <remarks>
	/// The ChainMapper class allows to use multiple Mapper classes within a single
	/// Map task.
	/// <p>
	/// The Mapper classes are invoked in a chained (or piped) fashion, the output of
	/// the first becomes the input of the second, and so on until the last Mapper,
	/// the output of the last Mapper will be written to the task's output.
	/// <p>
	/// The key functionality of this feature is that the Mappers in the chain do not
	/// need to be aware that they are executed in a chain. This enables having
	/// reusable specialized Mappers that can be combined to perform composite
	/// operations within a single task.
	/// <p>
	/// Special care has to be taken when creating chains that the key/values output
	/// by a Mapper are valid for the following Mapper in the chain. It is assumed
	/// all Mappers and the Reduce in the chain use maching output and input key and
	/// value classes as no conversion is done by the chaining code.
	/// <p>
	/// Using the ChainMapper and the ChainReducer classes is possible to compose
	/// Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
	/// immediate benefit of this pattern is a dramatic reduction in disk IO.
	/// <p>
	/// IMPORTANT: There is no need to specify the output key/value classes for the
	/// ChainMapper, this is done by the addMapper for the last mapper in the chain.
	/// <p>
	/// ChainMapper usage pattern:
	/// <p>
	/// <pre>
	/// ...
	/// conf.setJobName("chain");
	/// conf.setInputFormat(TextInputFormat.class);
	/// conf.setOutputFormat(TextOutputFormat.class);
	/// JobConf mapAConf = new JobConf(false);
	/// ...
	/// ChainMapper.addMapper(conf, AMap.class, LongWritable.class, Text.class,
	/// Text.class, Text.class, true, mapAConf);
	/// JobConf mapBConf = new JobConf(false);
	/// ...
	/// ChainMapper.addMapper(conf, BMap.class, Text.class, Text.class,
	/// LongWritable.class, Text.class, false, mapBConf);
	/// JobConf reduceConf = new JobConf(false);
	/// ...
	/// ChainReducer.setReducer(conf, XReduce.class, LongWritable.class, Text.class,
	/// Text.class, Text.class, true, reduceConf);
	/// ChainReducer.addMapper(conf, CMap.class, Text.class, Text.class,
	/// LongWritable.class, Text.class, false, null);
	/// ChainReducer.addMapper(conf, DMap.class, LongWritable.class, Text.class,
	/// LongWritable.class, LongWritable.class, true, null);
	/// FileInputFormat.setInputPaths(conf, inDir);
	/// FileOutputFormat.setOutputPath(conf, outDir);
	/// ...
	/// JobClient jc = new JobClient(conf);
	/// RunningJob job = jc.submitJob(conf);
	/// ...
	/// </pre>
	/// </remarks>
	public class ChainMapper : Mapper
	{
		/// <summary>Adds a Mapper class to the chain job's JobConf.</summary>
		/// <remarks>
		/// Adds a Mapper class to the chain job's JobConf.
		/// <p>
		/// It has to be specified how key and values are passed from one element of
		/// the chain to the next, by value or by reference. If a Mapper leverages the
		/// assumed semantics that the key and values are not modified by the collector
		/// 'by value' must be used. If the Mapper does not expect this semantics, as
		/// an optimization to avoid serialization and deserialization 'by reference'
		/// can be used.
		/// <p>
		/// For the added Mapper the configuration given for it,
		/// <code>mapperConf</code>, have precedence over the job's JobConf. This
		/// precedence is in effect when the task is running.
		/// <p>
		/// IMPORTANT: There is no need to specify the output key/value classes for the
		/// ChainMapper, this is done by the addMapper for the last mapper in the chain
		/// <p>
		/// </remarks>
		/// <param name="job">job's JobConf to add the Mapper class.</param>
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
		public static void AddMapper<K1, V1, K2, V2>(JobConf job, Type klass, Type inputKeyClass
			, Type inputValueClass, Type outputKeyClass, Type outputValueClass, bool byValue
			, JobConf mapperConf)
		{
			job.SetMapperClass(typeof(Org.Apache.Hadoop.Mapred.Lib.ChainMapper));
			job.SetMapOutputKeyClass(outputKeyClass);
			job.SetMapOutputValueClass(outputValueClass);
			Chain.AddMapper(true, job, klass, inputKeyClass, inputValueClass, outputKeyClass, 
				outputValueClass, byValue, mapperConf);
		}

		private Chain chain;

		/// <summary>Constructor.</summary>
		public ChainMapper()
		{
			chain = new Chain(true);
		}

		/// <summary>Configures the ChainMapper and all the Mappers in the chain.</summary>
		/// <remarks>
		/// Configures the ChainMapper and all the Mappers in the chain.
		/// <p>
		/// If this method is overriden <code>super.configure(...)</code> should be
		/// invoked at the beginning of the overwriter method.
		/// </remarks>
		public virtual void Configure(JobConf job)
		{
			chain.Configure(job);
		}

		/// <summary>Chains the <code>map(...)</code> methods of the Mappers in the chain.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(object key, object value, OutputCollector output, Reporter
			 reporter)
		{
			Mapper mapper = chain.GetFirstMap();
			if (mapper != null)
			{
				mapper.Map(key, value, chain.GetMapperCollector(0, output, reporter), reporter);
			}
		}

		/// <summary>Closes  the ChainMapper and all the Mappers in the chain.</summary>
		/// <remarks>
		/// Closes  the ChainMapper and all the Mappers in the chain.
		/// <p>
		/// If this method is overriden <code>super.close()</code> should be
		/// invoked at the end of the overwriter method.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			chain.Close();
		}
	}
}
