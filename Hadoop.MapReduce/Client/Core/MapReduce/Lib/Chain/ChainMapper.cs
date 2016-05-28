using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Chain
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
	/// </p>
	/// <p>
	/// The key functionality of this feature is that the Mappers in the chain do not
	/// need to be aware that they are executed in a chain. This enables having
	/// reusable specialized Mappers that can be combined to perform composite
	/// operations within a single task.
	/// </p>
	/// <p>
	/// Special care has to be taken when creating chains that the key/values output
	/// by a Mapper are valid for the following Mapper in the chain. It is assumed
	/// all Mappers and the Reduce in the chain use matching output and input key and
	/// value classes as no conversion is done by the chaining code.
	/// </p>
	/// <p>
	/// Using the ChainMapper and the ChainReducer classes is possible to compose
	/// Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
	/// immediate benefit of this pattern is a dramatic reduction in disk IO.
	/// </p>
	/// <p>
	/// IMPORTANT: There is no need to specify the output key/value classes for the
	/// ChainMapper, this is done by the addMapper for the last mapper in the chain.
	/// </p>
	/// ChainMapper usage pattern:
	/// <p>
	/// <pre>
	/// ...
	/// Job = new Job(conf);
	/// Configuration mapAConf = new Configuration(false);
	/// ...
	/// ChainMapper.addMapper(job, AMap.class, LongWritable.class, Text.class,
	/// Text.class, Text.class, true, mapAConf);
	/// Configuration mapBConf = new Configuration(false);
	/// ...
	/// ChainMapper.addMapper(job, BMap.class, Text.class, Text.class,
	/// LongWritable.class, Text.class, false, mapBConf);
	/// ...
	/// job.waitForComplettion(true);
	/// ...
	/// </pre>
	/// </remarks>
	public class ChainMapper<Keyin, Valuein, Keyout, Valueout> : Mapper<KEYIN, VALUEIN
		, KEYOUT, VALUEOUT>
	{
		/// <summary>
		/// Adds a
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// class to the chain mapper.
		/// <p>
		/// The key and values are passed from one element of the chain to the next, by
		/// value. For the added Mapper the configuration given for it,
		/// <code>mapperConf</code>, have precedence over the job's Configuration. This
		/// precedence is in effect when the task is running.
		/// </p>
		/// <p>
		/// IMPORTANT: There is no need to specify the output key/value classes for the
		/// ChainMapper, this is done by the addMapper for the last mapper in the chain
		/// </p>
		/// </summary>
		/// <param name="job">The job.</param>
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
		/// <exception cref="System.IO.IOException"/>
		public static void AddMapper(Job job, Type klass, Type inputKeyClass, Type inputValueClass
			, Type outputKeyClass, Type outputValueClass, Configuration mapperConf)
		{
			job.SetMapperClass(typeof(ChainMapper));
			job.SetMapOutputKeyClass(outputKeyClass);
			job.SetMapOutputValueClass(outputValueClass);
			Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain.AddMapper(true, job, klass, inputKeyClass
				, inputValueClass, outputKeyClass, outputValueClass, mapperConf);
		}

		private Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain chain;

		protected internal override void Setup(Mapper.Context context)
		{
			chain = new Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain(true);
			chain.Setup(context.GetConfiguration());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Run(Mapper.Context context)
		{
			Setup(context);
			int numMappers = chain.GetAllMappers().Count;
			if (numMappers == 0)
			{
				return;
			}
			Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>> inputqueue;
			Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>> outputqueue;
			if (numMappers == 1)
			{
				chain.RunMapper(context, 0);
			}
			else
			{
				// add all the mappers with proper context
				// add first mapper
				outputqueue = chain.CreateBlockingQueue();
				chain.AddMapper(context, outputqueue, 0);
				// add other mappers
				for (int i = 1; i < numMappers - 1; i++)
				{
					inputqueue = outputqueue;
					outputqueue = chain.CreateBlockingQueue();
					chain.AddMapper(inputqueue, outputqueue, context, i);
				}
				// add last mapper
				chain.AddMapper(outputqueue, context, numMappers - 1);
			}
			// start all threads
			chain.StartAllThreads();
			// wait for all threads
			chain.JoinAllThreads();
		}
	}
}
