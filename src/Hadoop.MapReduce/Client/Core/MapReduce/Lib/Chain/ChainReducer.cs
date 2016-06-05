using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Chain
{
	/// <summary>
	/// The ChainReducer class allows to chain multiple Mapper classes after a
	/// Reducer within the Reducer task.
	/// </summary>
	/// <remarks>
	/// The ChainReducer class allows to chain multiple Mapper classes after a
	/// Reducer within the Reducer task.
	/// <p>
	/// For each record output by the Reducer, the Mapper classes are invoked in a
	/// chained (or piped) fashion. The output of the reducer becomes the input of
	/// the first mapper and output of first becomes the input of the second, and so
	/// on until the last Mapper, the output of the last Mapper will be written to
	/// the task's output.
	/// </p>
	/// <p>
	/// The key functionality of this feature is that the Mappers in the chain do not
	/// need to be aware that they are executed after the Reducer or in a chain. This
	/// enables having reusable specialized Mappers that can be combined to perform
	/// composite operations within a single task.
	/// </p>
	/// <p>
	/// Special care has to be taken when creating chains that the key/values output
	/// by a Mapper are valid for the following Mapper in the chain. It is assumed
	/// all Mappers and the Reduce in the chain use matching output and input key and
	/// value classes as no conversion is done by the chaining code.
	/// </p>
	/// <p> Using the ChainMapper and the ChainReducer classes is possible to
	/// compose Map/Reduce jobs that look like <code>[MAP+ / REDUCE MAP*]</code>. And
	/// immediate benefit of this pattern is a dramatic reduction in disk IO. </p>
	/// <p>
	/// IMPORTANT: There is no need to specify the output key/value classes for the
	/// ChainReducer, this is done by the setReducer or the addMapper for the last
	/// element in the chain.
	/// </p>
	/// ChainReducer usage pattern:
	/// <p>
	/// <pre>
	/// ...
	/// Job = new Job(conf);
	/// ....
	/// Configuration reduceConf = new Configuration(false);
	/// ...
	/// ChainReducer.setReducer(job, XReduce.class, LongWritable.class, Text.class,
	/// Text.class, Text.class, true, reduceConf);
	/// ChainReducer.addMapper(job, CMap.class, Text.class, Text.class,
	/// LongWritable.class, Text.class, false, null);
	/// ChainReducer.addMapper(job, DMap.class, LongWritable.class, Text.class,
	/// LongWritable.class, LongWritable.class, true, null);
	/// ...
	/// job.waitForCompletion(true);
	/// ...
	/// </pre>
	/// </remarks>
	public class ChainReducer<Keyin, Valuein, Keyout, Valueout> : Reducer<KEYIN, VALUEIN
		, KEYOUT, VALUEOUT>
	{
		/// <summary>
		/// Sets the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"
		/// 	/>
		/// class to the chain job.
		/// <p>
		/// The key and values are passed from one element of the chain to the next, by
		/// value. For the added Reducer the configuration given for it,
		/// <code>reducerConf</code>, have precedence over the job's Configuration.
		/// This precedence is in effect when the task is running.
		/// </p>
		/// <p>
		/// IMPORTANT: There is no need to specify the output key/value classes for the
		/// ChainReducer, this is done by the setReducer or the addMapper for the last
		/// element in the chain.
		/// </p>
		/// </summary>
		/// <param name="job">the job</param>
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
		public static void SetReducer(Job job, Type klass, Type inputKeyClass, Type inputValueClass
			, Type outputKeyClass, Type outputValueClass, Configuration reducerConf)
		{
			job.SetReducerClass(typeof(ChainReducer));
			job.SetOutputKeyClass(outputKeyClass);
			job.SetOutputValueClass(outputValueClass);
			Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain.SetReducer(job, klass, inputKeyClass, 
				inputValueClass, outputKeyClass, outputValueClass, reducerConf);
		}

		/// <summary>
		/// Adds a
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// class to the chain reducer.
		/// <p>
		/// The key and values are passed from one element of the chain to the next, by
		/// value For the added Mapper the configuration given for it,
		/// <code>mapperConf</code>, have precedence over the job's Configuration. This
		/// precedence is in effect when the task is running.
		/// </p>
		/// <p>
		/// IMPORTANT: There is no need to specify the output key/value classes for the
		/// ChainMapper, this is done by the addMapper for the last mapper in the
		/// chain.
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
			job.SetOutputKeyClass(outputKeyClass);
			job.SetOutputValueClass(outputValueClass);
			Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain.AddMapper(false, job, klass, inputKeyClass
				, inputValueClass, outputKeyClass, outputValueClass, mapperConf);
		}

		private Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain chain;

		protected internal override void Setup(Reducer.Context context)
		{
			chain = new Org.Apache.Hadoop.Mapreduce.Lib.Chain.Chain(false);
			chain.Setup(context.GetConfiguration());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Run(Reducer.Context context)
		{
			Setup(context);
			// if no reducer is set, just do nothing
			if (chain.GetReducer() == null)
			{
				return;
			}
			int numMappers = chain.GetAllMappers().Count;
			// if there are no mappers in chain, run the reducer
			if (numMappers == 0)
			{
				chain.RunReducer(context);
				return;
			}
			// add reducer and all mappers with proper context
			Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>> inputqueue;
			Chain.ChainBlockingQueue<Chain.KeyValuePair<object, object>> outputqueue;
			// add reducer
			outputqueue = chain.CreateBlockingQueue();
			chain.AddReducer(context, outputqueue);
			// add all mappers except last one
			for (int i = 0; i < numMappers - 1; i++)
			{
				inputqueue = outputqueue;
				outputqueue = chain.CreateBlockingQueue();
				chain.AddMapper(inputqueue, outputqueue, context, i);
			}
			// add last mapper
			chain.AddMapper(outputqueue, context, numMappers - 1);
			// start all threads
			chain.StartAllThreads();
			// wait for all threads
			chain.JoinAllThreads();
		}
	}
}
