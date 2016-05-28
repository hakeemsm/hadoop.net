using System;
using System.IO;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>The main entry point and job submitter.</summary>
	/// <remarks>
	/// The main entry point and job submitter. It may either be used as a command
	/// line-based or API-based method to launch Pipes jobs.
	/// </remarks>
	public class Submitter : Configured, Tool
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Pipes.Submitter
			));

		public const string PreserveCommandfile = "mapreduce.pipes.commandfile.preserve";

		public const string Executable = "mapreduce.pipes.executable";

		public const string Interpretor = "mapreduce.pipes.executable.interpretor";

		public const string IsJavaMap = "mapreduce.pipes.isjavamapper";

		public const string IsJavaRr = "mapreduce.pipes.isjavarecordreader";

		public const string IsJavaRw = "mapreduce.pipes.isjavarecordwriter";

		public const string IsJavaReduce = "mapreduce.pipes.isjavareducer";

		public const string Partitioner = "mapreduce.pipes.partitioner";

		public const string InputFormat = "mapreduce.pipes.inputformat";

		public const string Port = "mapreduce.pipes.command.port";

		public Submitter()
			: this(new Configuration())
		{
		}

		public Submitter(Configuration conf)
		{
			SetConf(conf);
		}

		/// <summary>Get the URI of the application's executable.</summary>
		/// <param name="conf"/>
		/// <returns>the URI where the application's executable is located</returns>
		public static string GetExecutable(JobConf conf)
		{
			return conf.Get(Org.Apache.Hadoop.Mapred.Pipes.Submitter.Executable);
		}

		/// <summary>Set the URI for the application's executable.</summary>
		/// <remarks>
		/// Set the URI for the application's executable. Normally this is a hdfs:
		/// location.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="executable">The URI of the application's executable.</param>
		public static void SetExecutable(JobConf conf, string executable)
		{
			conf.Set(Org.Apache.Hadoop.Mapred.Pipes.Submitter.Executable, executable);
		}

		/// <summary>Set whether the job is using a Java RecordReader.</summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="value">the new value</param>
		public static void SetIsJavaRecordReader(JobConf conf, bool value)
		{
			conf.SetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaRr, value);
		}

		/// <summary>Check whether the job is using a Java RecordReader</summary>
		/// <param name="conf">the configuration to check</param>
		/// <returns>is it a Java RecordReader?</returns>
		public static bool GetIsJavaRecordReader(JobConf conf)
		{
			return conf.GetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaRr, false);
		}

		/// <summary>Set whether the Mapper is written in Java.</summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="value">the new value</param>
		public static void SetIsJavaMapper(JobConf conf, bool value)
		{
			conf.SetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaMap, value);
		}

		/// <summary>Check whether the job is using a Java Mapper.</summary>
		/// <param name="conf">the configuration to check</param>
		/// <returns>is it a Java Mapper?</returns>
		public static bool GetIsJavaMapper(JobConf conf)
		{
			return conf.GetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaMap, false);
		}

		/// <summary>Set whether the Reducer is written in Java.</summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="value">the new value</param>
		public static void SetIsJavaReducer(JobConf conf, bool value)
		{
			conf.SetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaReduce, value);
		}

		/// <summary>Check whether the job is using a Java Reducer.</summary>
		/// <param name="conf">the configuration to check</param>
		/// <returns>is it a Java Reducer?</returns>
		public static bool GetIsJavaReducer(JobConf conf)
		{
			return conf.GetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaReduce, false
				);
		}

		/// <summary>Set whether the job will use a Java RecordWriter.</summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="value">the new value to set</param>
		public static void SetIsJavaRecordWriter(JobConf conf, bool value)
		{
			conf.SetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaRw, value);
		}

		/// <summary>Will the reduce use a Java RecordWriter?</summary>
		/// <param name="conf">the configuration to check</param>
		/// <returns>true, if the output of the job will be written by Java</returns>
		public static bool GetIsJavaRecordWriter(JobConf conf)
		{
			return conf.GetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.IsJavaRw, false);
		}

		/// <summary>
		/// Set the configuration, if it doesn't already have a value for the given
		/// key.
		/// </summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="key">the key to set</param>
		/// <param name="value">the new "default" value to set</param>
		private static void SetIfUnset(JobConf conf, string key, string value)
		{
			if (conf.Get(key) == null)
			{
				conf.Set(key, value);
			}
		}

		/// <summary>Save away the user's original partitioner before we override it.</summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="cls">the user's partitioner class</param>
		internal static void SetJavaPartitioner(JobConf conf, Type cls)
		{
			conf.Set(Org.Apache.Hadoop.Mapred.Pipes.Submitter.Partitioner, cls.FullName);
		}

		/// <summary>Get the user's original partitioner.</summary>
		/// <param name="conf">the configuration to look in</param>
		/// <returns>the class that the user submitted</returns>
		internal static Type GetJavaPartitioner(JobConf conf)
		{
			return conf.GetClass<Partitioner>(Org.Apache.Hadoop.Mapred.Pipes.Submitter.Partitioner
				, typeof(HashPartitioner));
		}

		/// <summary>
		/// Does the user want to keep the command file for debugging? If this is
		/// true, pipes will write a copy of the command data to a file in the
		/// task directory named "downlink.data", which may be used to run the C++
		/// program under the debugger.
		/// </summary>
		/// <remarks>
		/// Does the user want to keep the command file for debugging? If this is
		/// true, pipes will write a copy of the command data to a file in the
		/// task directory named "downlink.data", which may be used to run the C++
		/// program under the debugger. You probably also want to set
		/// JobConf.setKeepFailedTaskFiles(true) to keep the entire directory from
		/// being deleted.
		/// To run using the data file, set the environment variable
		/// "mapreduce.pipes.commandfile" to point to the file.
		/// </remarks>
		/// <param name="conf">the configuration to check</param>
		/// <returns>will the framework save the command file?</returns>
		public static bool GetKeepCommandFile(JobConf conf)
		{
			return conf.GetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.PreserveCommandfile
				, false);
		}

		/// <summary>Set whether to keep the command file for debugging</summary>
		/// <param name="conf">the configuration to modify</param>
		/// <param name="keep">the new value</param>
		public static void SetKeepCommandFile(JobConf conf, bool keep)
		{
			conf.SetBoolean(Org.Apache.Hadoop.Mapred.Pipes.Submitter.PreserveCommandfile, keep
				);
		}

		/// <summary>Submit a job to the map/reduce cluster.</summary>
		/// <remarks>
		/// Submit a job to the map/reduce cluster. All of the necessary modifications
		/// to the job to run under pipes are made to the configuration.
		/// </remarks>
		/// <param name="conf">the job to submit to the cluster (MODIFIED)</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use RunJob(Org.Apache.Hadoop.Mapred.JobConf)")]
		public static RunningJob SubmitJob(JobConf conf)
		{
			return RunJob(conf);
		}

		/// <summary>Submit a job to the map/reduce cluster.</summary>
		/// <remarks>
		/// Submit a job to the map/reduce cluster. All of the necessary modifications
		/// to the job to run under pipes are made to the configuration.
		/// </remarks>
		/// <param name="conf">the job to submit to the cluster (MODIFIED)</param>
		/// <exception cref="System.IO.IOException"/>
		public static RunningJob RunJob(JobConf conf)
		{
			SetupPipesJob(conf);
			return JobClient.RunJob(conf);
		}

		/// <summary>Submit a job to the Map-Reduce framework.</summary>
		/// <remarks>
		/// Submit a job to the Map-Reduce framework.
		/// This returns a handle to the
		/// <see cref="Org.Apache.Hadoop.Mapred.RunningJob"/>
		/// which can be used to track
		/// the running-job.
		/// </remarks>
		/// <param name="conf">the job configuration.</param>
		/// <returns>
		/// a handle to the
		/// <see cref="Org.Apache.Hadoop.Mapred.RunningJob"/>
		/// which can be used to track the
		/// running-job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static RunningJob JobSubmit(JobConf conf)
		{
			SetupPipesJob(conf);
			return new JobClient(conf).SubmitJob(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SetupPipesJob(JobConf conf)
		{
			// default map output types to Text
			if (!GetIsJavaMapper(conf))
			{
				conf.SetMapRunnerClass(typeof(PipesMapRunner));
				// Save the user's partitioner and hook in our's.
				SetJavaPartitioner(conf, conf.GetPartitionerClass());
				conf.SetPartitionerClass(typeof(PipesPartitioner));
			}
			if (!GetIsJavaReducer(conf))
			{
				conf.SetReducerClass(typeof(PipesReducer));
				if (!GetIsJavaRecordWriter(conf))
				{
					conf.SetOutputFormat(typeof(NullOutputFormat));
				}
			}
			string textClassname = typeof(Text).FullName;
			SetIfUnset(conf, MRJobConfig.MapOutputKeyClass, textClassname);
			SetIfUnset(conf, MRJobConfig.MapOutputValueClass, textClassname);
			SetIfUnset(conf, MRJobConfig.OutputKeyClass, textClassname);
			SetIfUnset(conf, MRJobConfig.OutputValueClass, textClassname);
			// Use PipesNonJavaInputFormat if necessary to handle progress reporting
			// from C++ RecordReaders ...
			if (!GetIsJavaRecordReader(conf) && !GetIsJavaMapper(conf))
			{
				conf.SetClass(Org.Apache.Hadoop.Mapred.Pipes.Submitter.InputFormat, conf.GetInputFormat
					().GetType(), typeof(InputFormat));
				conf.SetInputFormat(typeof(PipesNonJavaInputFormat));
			}
			string exec = GetExecutable(conf);
			if (exec == null)
			{
				throw new ArgumentException("No application program defined.");
			}
			// add default debug script only when executable is expressed as
			// <path>#<executable>
			if (exec.Contains("#"))
			{
				// set default gdb commands for map and reduce task 
				string defScript = "$HADOOP_PREFIX/src/c++/pipes/debug/pipes-default-script";
				SetIfUnset(conf, MRJobConfig.MapDebugScript, defScript);
				SetIfUnset(conf, MRJobConfig.ReduceDebugScript, defScript);
			}
			URI[] fileCache = DistributedCache.GetCacheFiles(conf);
			if (fileCache == null)
			{
				fileCache = new URI[1];
			}
			else
			{
				URI[] tmp = new URI[fileCache.Length + 1];
				System.Array.Copy(fileCache, 0, tmp, 1, fileCache.Length);
				fileCache = tmp;
			}
			try
			{
				fileCache[0] = new URI(exec);
			}
			catch (URISyntaxException e)
			{
				IOException ie = new IOException("Problem parsing execable URI " + exec);
				Sharpen.Extensions.InitCause(ie, e);
				throw ie;
			}
			DistributedCache.SetCacheFiles(fileCache, conf);
		}

		/// <summary>A command line parser for the CLI-based Pipes job submitter.</summary>
		internal class CommandLineParser
		{
			private Options options = new Options();

			internal virtual void AddOption(string longName, bool required, string description
				, string paramName)
			{
				Option option = OptionBuilder.Create(longName);
				options.AddOption(option);
			}

			internal virtual void AddArgument(string name, bool required, string description)
			{
				Option option = OptionBuilder.Create();
				options.AddOption(option);
			}

			internal virtual Parser CreateParser()
			{
				Parser result = new BasicParser();
				return result;
			}

			internal virtual void PrintUsage()
			{
				// The CLI package should do this for us, but I can't figure out how
				// to make it print something reasonable.
				System.Console.Out.WriteLine("bin/hadoop pipes");
				System.Console.Out.WriteLine("  [-input <path>] // Input directory");
				System.Console.Out.WriteLine("  [-output <path>] // Output directory");
				System.Console.Out.WriteLine("  [-jar <jar file> // jar filename");
				System.Console.Out.WriteLine("  [-inputformat <class>] // InputFormat class");
				System.Console.Out.WriteLine("  [-map <class>] // Java Map class");
				System.Console.Out.WriteLine("  [-partitioner <class>] // Java Partitioner");
				System.Console.Out.WriteLine("  [-reduce <class>] // Java Reduce class");
				System.Console.Out.WriteLine("  [-writer <class>] // Java RecordWriter");
				System.Console.Out.WriteLine("  [-program <executable>] // executable URI");
				System.Console.Out.WriteLine("  [-reduces <num>] // number of reduces");
				System.Console.Out.WriteLine("  [-lazyOutput <true/false>] // createOutputLazily"
					);
				System.Console.Out.WriteLine();
				GenericOptionsParser.PrintGenericCommandUsage(System.Console.Out);
			}
		}

		/// <exception cref="System.TypeLoadException"/>
		private static Type GetClass<InterfaceType>(CommandLine cl, string key, JobConf conf
			)
		{
			System.Type cls = typeof(InterfaceType);
			return conf.GetClassByName(cl.GetOptionValue(key)).AsSubclass(cls);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Submitter.CommandLineParser cli = new Submitter.CommandLineParser();
			if (args.Length == 0)
			{
				cli.PrintUsage();
				return 1;
			}
			cli.AddOption("input", false, "input path to the maps", "path");
			cli.AddOption("output", false, "output path from the reduces", "path");
			cli.AddOption("jar", false, "job jar file", "path");
			cli.AddOption("inputformat", false, "java classname of InputFormat", "class");
			//cli.addArgument("javareader", false, "is the RecordReader in Java");
			cli.AddOption("map", false, "java classname of Mapper", "class");
			cli.AddOption("partitioner", false, "java classname of Partitioner", "class");
			cli.AddOption("reduce", false, "java classname of Reducer", "class");
			cli.AddOption("writer", false, "java classname of OutputFormat", "class");
			cli.AddOption("program", false, "URI to application executable", "class");
			cli.AddOption("reduces", false, "number of reduces", "num");
			cli.AddOption("jobconf", false, "\"n1=v1,n2=v2,..\" (Deprecated) Optional. Add or override a JobConf property."
				, "key=val");
			cli.AddOption("lazyOutput", false, "Optional. Create output lazily", "boolean");
			Parser parser = cli.CreateParser();
			try
			{
				GenericOptionsParser genericParser = new GenericOptionsParser(GetConf(), args);
				CommandLine results = parser.Parse(cli.options, genericParser.GetRemainingArgs());
				JobConf job = new JobConf(GetConf());
				if (results.HasOption("input"))
				{
					FileInputFormat.SetInputPaths(job, results.GetOptionValue("input"));
				}
				if (results.HasOption("output"))
				{
					FileOutputFormat.SetOutputPath(job, new Path(results.GetOptionValue("output")));
				}
				if (results.HasOption("jar"))
				{
					job.SetJar(results.GetOptionValue("jar"));
				}
				if (results.HasOption("inputformat"))
				{
					SetIsJavaRecordReader(job, true);
					job.SetInputFormat(GetClass<InputFormat>(results, "inputformat", job));
				}
				if (results.HasOption("javareader"))
				{
					SetIsJavaRecordReader(job, true);
				}
				if (results.HasOption("map"))
				{
					SetIsJavaMapper(job, true);
					job.SetMapperClass(GetClass<Mapper>(results, "map", job));
				}
				if (results.HasOption("partitioner"))
				{
					job.SetPartitionerClass(GetClass<Partitioner>(results, "partitioner", job));
				}
				if (results.HasOption("reduce"))
				{
					SetIsJavaReducer(job, true);
					job.SetReducerClass(GetClass<Reducer>(results, "reduce", job));
				}
				if (results.HasOption("reduces"))
				{
					job.SetNumReduceTasks(System.Convert.ToInt32(results.GetOptionValue("reduces")));
				}
				if (results.HasOption("writer"))
				{
					SetIsJavaRecordWriter(job, true);
					job.SetOutputFormat(GetClass<OutputFormat>(results, "writer", job));
				}
				if (results.HasOption("lazyOutput"))
				{
					if (System.Boolean.Parse(results.GetOptionValue("lazyOutput")))
					{
						LazyOutputFormat.SetOutputFormatClass(job, job.GetOutputFormat().GetType());
					}
				}
				if (results.HasOption("program"))
				{
					SetExecutable(job, results.GetOptionValue("program"));
				}
				if (results.HasOption("jobconf"))
				{
					Log.Warn("-jobconf option is deprecated, please use -D instead.");
					string options = results.GetOptionValue("jobconf");
					StringTokenizer tokenizer = new StringTokenizer(options, ",");
					while (tokenizer.HasMoreTokens())
					{
						string keyVal = tokenizer.NextToken().Trim();
						string[] keyValSplit = keyVal.Split("=");
						job.Set(keyValSplit[0], keyValSplit[1]);
					}
				}
				// if they gave us a jar file, include it into the class path
				string jarFile = job.GetJar();
				if (jarFile != null)
				{
					Uri[] urls = new Uri[] { FileSystem.GetLocal(job).PathToFile(new Path(jarFile)).ToURL
						() };
					//FindBugs complains that creating a URLClassLoader should be
					//in a doPrivileged() block. 
					ClassLoader loader = AccessController.DoPrivileged(new _PrivilegedAction_494(urls
						));
					job.SetClassLoader(loader);
				}
				RunJob(job);
				return 0;
			}
			catch (ParseException pe)
			{
				Log.Info("Error : " + pe);
				cli.PrintUsage();
				return 1;
			}
		}

		private sealed class _PrivilegedAction_494 : PrivilegedAction<ClassLoader>
		{
			public _PrivilegedAction_494(Uri[] urls)
			{
				this.urls = urls;
			}

			public ClassLoader Run()
			{
				return new URLClassLoader(urls);
			}

			private readonly Uri[] urls;
		}

		/// <summary>Submit a pipes job based on the command line arguments.</summary>
		/// <param name="args"/>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int exitCode = new Submitter().Run(args);
			ExitUtil.Terminate(exitCode);
		}
	}
}
