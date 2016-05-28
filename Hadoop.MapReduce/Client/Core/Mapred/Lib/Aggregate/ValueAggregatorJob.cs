using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Jobcontrol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.Aggregate
{
	/// <summary>
	/// This is the main class for creating a map/reduce job using Aggregate
	/// framework.
	/// </summary>
	/// <remarks>
	/// This is the main class for creating a map/reduce job using Aggregate
	/// framework. The Aggregate is a specialization of map/reduce framework,
	/// specilizing for performing various simple aggregations.
	/// Generally speaking, in order to implement an application using Map/Reduce
	/// model, the developer is to implement Map and Reduce functions (and possibly
	/// combine function). However, a lot of applications related to counting and
	/// statistics computing have very similar characteristics. Aggregate abstracts
	/// out the general patterns of these functions and implementing those patterns.
	/// In particular, the package provides generic mapper/redducer/combiner classes,
	/// and a set of built-in value aggregators, and a generic utility class that
	/// helps user create map/reduce jobs using the generic class. The built-in
	/// aggregators include:
	/// sum over numeric values count the number of distinct values compute the
	/// histogram of values compute the minimum, maximum, media,average, standard
	/// deviation of numeric values
	/// The developer using Aggregate will need only to provide a plugin class
	/// conforming to the following interface:
	/// public interface ValueAggregatorDescriptor { public ArrayList&lt;Entry&gt;
	/// generateKeyValPairs(Object key, Object value); public void
	/// configure(JobConfjob); }
	/// The package also provides a base class, ValueAggregatorBaseDescriptor,
	/// implementing the above interface. The user can extend the base class and
	/// implement generateKeyValPairs accordingly.
	/// The primary work of generateKeyValPairs is to emit one or more key/value
	/// pairs based on the input key/value pair. The key in an output key/value pair
	/// encode two pieces of information: aggregation type and aggregation id. The
	/// value will be aggregated onto the aggregation id according the aggregation
	/// type.
	/// This class offers a function to generate a map/reduce job using Aggregate
	/// framework. The function takes the following parameters: input directory spec
	/// input format (text or sequence file) output directory a file specifying the
	/// user plugin class
	/// </remarks>
	public class ValueAggregatorJob
	{
		/// <exception cref="System.IO.IOException"/>
		public static JobControl CreateValueAggregatorJobs(string[] args, Type[] descriptors
			)
		{
			JobControl theControl = new JobControl("ValueAggregatorJobs");
			AList<Job> dependingJobs = new AList<Job>();
			JobConf aJobConf = CreateValueAggregatorJob(args);
			if (descriptors != null)
			{
				SetAggregatorDescriptors(aJobConf, descriptors);
			}
			Job aJob = new Job(aJobConf, dependingJobs);
			theControl.AddJob(aJob);
			return theControl;
		}

		/// <exception cref="System.IO.IOException"/>
		public static JobControl CreateValueAggregatorJobs(string[] args)
		{
			return CreateValueAggregatorJobs(args, null);
		}

		/// <summary>Create an Aggregate based map/reduce job.</summary>
		/// <param name="args">
		/// the arguments used for job creation. Generic hadoop
		/// arguments are accepted.
		/// </param>
		/// <param name="caller">the the caller class.</param>
		/// <returns>a JobConf object ready for submission.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Util.GenericOptionsParser"/>
		public static JobConf CreateValueAggregatorJob(string[] args, Type caller)
		{
			Configuration conf = new Configuration();
			GenericOptionsParser genericParser = new GenericOptionsParser(conf, args);
			args = genericParser.GetRemainingArgs();
			if (args.Length < 2)
			{
				System.Console.Out.WriteLine("usage: inputDirs outDir " + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]"
					);
				GenericOptionsParser.PrintGenericCommandUsage(System.Console.Out);
				System.Environment.Exit(1);
			}
			string inputDir = args[0];
			string outputDir = args[1];
			int numOfReducers = 1;
			if (args.Length > 2)
			{
				numOfReducers = System.Convert.ToInt32(args[2]);
			}
			Type theInputFormat = typeof(TextInputFormat);
			if (args.Length > 3 && args[3].CompareToIgnoreCase("textinputformat") == 0)
			{
				theInputFormat = typeof(TextInputFormat);
			}
			else
			{
				theInputFormat = typeof(SequenceFileInputFormat);
			}
			Path specFile = null;
			if (args.Length > 4)
			{
				specFile = new Path(args[4]);
			}
			string jobName = string.Empty;
			if (args.Length > 5)
			{
				jobName = args[5];
			}
			JobConf theJob = new JobConf(conf);
			if (specFile != null)
			{
				theJob.AddResource(specFile);
			}
			string userJarFile = theJob.Get("user.jar.file");
			if (userJarFile == null)
			{
				theJob.SetJarByClass(caller != null ? caller : typeof(ValueAggregatorJob));
			}
			else
			{
				theJob.SetJar(userJarFile);
			}
			theJob.SetJobName("ValueAggregatorJob: " + jobName);
			FileInputFormat.AddInputPaths(theJob, inputDir);
			theJob.SetInputFormat(theInputFormat);
			theJob.SetMapperClass(typeof(ValueAggregatorMapper));
			FileOutputFormat.SetOutputPath(theJob, new Path(outputDir));
			theJob.SetOutputFormat(typeof(TextOutputFormat));
			theJob.SetMapOutputKeyClass(typeof(Text));
			theJob.SetMapOutputValueClass(typeof(Text));
			theJob.SetOutputKeyClass(typeof(Text));
			theJob.SetOutputValueClass(typeof(Text));
			theJob.SetReducerClass(typeof(ValueAggregatorReducer));
			theJob.SetCombinerClass(typeof(ValueAggregatorCombiner));
			theJob.SetNumMapTasks(1);
			theJob.SetNumReduceTasks(numOfReducers);
			return theJob;
		}

		/// <summary>Create an Aggregate based map/reduce job.</summary>
		/// <param name="args">
		/// the arguments used for job creation. Generic hadoop
		/// arguments are accepted.
		/// </param>
		/// <returns>a JobConf object ready for submission.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Util.GenericOptionsParser"/>
		public static JobConf CreateValueAggregatorJob(string[] args)
		{
			return CreateValueAggregatorJob(args, typeof(ValueAggregator));
		}

		/// <exception cref="System.IO.IOException"/>
		public static JobConf CreateValueAggregatorJob(string[] args, Type[] descriptors)
		{
			JobConf job = CreateValueAggregatorJob(args);
			SetAggregatorDescriptors(job, descriptors);
			return job;
		}

		public static void SetAggregatorDescriptors(JobConf job, Type[] descriptors)
		{
			job.SetInt("aggregator.descriptor.num", descriptors.Length);
			//specify the aggregator descriptors
			for (int i = 0; i < descriptors.Length; i++)
			{
				job.Set("aggregator.descriptor." + i, "UserDefined," + descriptors[i].FullName);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static JobConf CreateValueAggregatorJob(string[] args, Type[] descriptors, 
			Type caller)
		{
			JobConf job = CreateValueAggregatorJob(args, caller);
			SetAggregatorDescriptors(job, descriptors);
			return job;
		}

		/// <summary>create and run an Aggregate based map/reduce job.</summary>
		/// <param name="args">the arguments used for job creation</param>
		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			JobConf job = ValueAggregatorJob.CreateValueAggregatorJob(args);
			JobClient.RunJob(job);
		}
	}
}
