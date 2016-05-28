using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Aggregate
{
	/// <summary>
	/// This is the main class for creating a map/reduce job using Aggregate
	/// framework.
	/// </summary>
	/// <remarks>
	/// This is the main class for creating a map/reduce job using Aggregate
	/// framework. The Aggregate is a specialization of map/reduce framework,
	/// specializing for performing various simple aggregations.
	/// Generally speaking, in order to implement an application using Map/Reduce
	/// model, the developer is to implement Map and Reduce functions (and possibly
	/// combine function). However, a lot of applications related to counting and
	/// statistics computing have very similar characteristics. Aggregate abstracts
	/// out the general patterns of these functions and implementing those patterns.
	/// In particular, the package provides generic mapper/redducer/combiner
	/// classes, and a set of built-in value aggregators, and a generic utility
	/// class that helps user create map/reduce jobs using the generic class.
	/// The built-in aggregators include:
	/// sum over numeric values count the number of distinct values compute the
	/// histogram of values compute the minimum, maximum, media,average, standard
	/// deviation of numeric values
	/// The developer using Aggregate will need only to provide a plugin class
	/// conforming to the following interface:
	/// public interface ValueAggregatorDescriptor { public ArrayList&lt;Entry&gt;
	/// generateKeyValPairs(Object key, Object value); public void
	/// configure(Configuration conf); }
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
			AList<ControlledJob> dependingJobs = new AList<ControlledJob>();
			Configuration conf = new Configuration();
			if (descriptors != null)
			{
				conf = SetAggregatorDescriptors(descriptors);
			}
			Job job = CreateValueAggregatorJob(conf, args);
			ControlledJob cjob = new ControlledJob(job, dependingJobs);
			theControl.AddJob(cjob);
			return theControl;
		}

		/// <exception cref="System.IO.IOException"/>
		public static JobControl CreateValueAggregatorJobs(string[] args)
		{
			return CreateValueAggregatorJobs(args, null);
		}

		/// <summary>Create an Aggregate based map/reduce job.</summary>
		/// <param name="conf">The configuration for job</param>
		/// <param name="args">
		/// the arguments used for job creation. Generic hadoop
		/// arguments are accepted.
		/// </param>
		/// <returns>a Job object ready for submission.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.Util.GenericOptionsParser"/>
		public static Job CreateValueAggregatorJob(Configuration conf, string[] args)
		{
			GenericOptionsParser genericParser = new GenericOptionsParser(conf, args);
			args = genericParser.GetRemainingArgs();
			if (args.Length < 2)
			{
				System.Console.Out.WriteLine("usage: inputDirs outDir " + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]"
					);
				GenericOptionsParser.PrintGenericCommandUsage(System.Console.Out);
				System.Environment.Exit(2);
			}
			string inputDir = args[0];
			string outputDir = args[1];
			int numOfReducers = 1;
			if (args.Length > 2)
			{
				numOfReducers = System.Convert.ToInt32(args[2]);
			}
			Type theInputFormat = null;
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
			if (specFile != null)
			{
				conf.AddResource(specFile);
			}
			string userJarFile = conf.Get(ValueAggregatorJobBase.UserJar);
			if (userJarFile != null)
			{
				conf.Set(MRJobConfig.Jar, userJarFile);
			}
			Job theJob = Job.GetInstance(conf);
			if (userJarFile == null)
			{
				theJob.SetJarByClass(typeof(ValueAggregator));
			}
			theJob.SetJobName("ValueAggregatorJob: " + jobName);
			FileInputFormat.AddInputPaths(theJob, inputDir);
			theJob.SetInputFormatClass(theInputFormat);
			theJob.SetMapperClass(typeof(ValueAggregatorMapper));
			FileOutputFormat.SetOutputPath(theJob, new Path(outputDir));
			theJob.SetOutputFormatClass(typeof(TextOutputFormat));
			theJob.SetMapOutputKeyClass(typeof(Text));
			theJob.SetMapOutputValueClass(typeof(Text));
			theJob.SetOutputKeyClass(typeof(Text));
			theJob.SetOutputValueClass(typeof(Text));
			theJob.SetReducerClass(typeof(ValueAggregatorReducer));
			theJob.SetCombinerClass(typeof(ValueAggregatorCombiner));
			theJob.SetNumReduceTasks(numOfReducers);
			return theJob;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Job CreateValueAggregatorJob(string[] args, Type[] descriptors)
		{
			return CreateValueAggregatorJob(SetAggregatorDescriptors(descriptors), args);
		}

		public static Configuration SetAggregatorDescriptors(Type[] descriptors)
		{
			Configuration conf = new Configuration();
			conf.SetInt(ValueAggregatorJobBase.DescriptorNum, descriptors.Length);
			//specify the aggregator descriptors
			for (int i = 0; i < descriptors.Length; i++)
			{
				conf.Set(ValueAggregatorJobBase.Descriptor + i, "UserDefined," + descriptors[i].FullName
					);
			}
			return conf;
		}

		/// <summary>create and run an Aggregate based map/reduce job.</summary>
		/// <param name="args">the arguments used for job creation</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public static void Main(string[] args)
		{
			Job job = ValueAggregatorJob.CreateValueAggregatorJob(new Configuration(), args);
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			System.Environment.Exit(ret);
		}
	}
}
