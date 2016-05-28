using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Examples.PI.Math;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples.PI
{
	/// <summary>The main class for computing sums using map/reduce jobs.</summary>
	/// <remarks>
	/// The main class for computing sums using map/reduce jobs.
	/// A sum is partitioned into jobs.
	/// A job may be executed on the map-side or on the reduce-side.
	/// A map-side job has multiple maps and zero reducer.
	/// A reduce-side job has one map and multiple reducers.
	/// Depending on the clusters status in runtime,
	/// a mix-type job may be executed on either side.
	/// </remarks>
	public sealed class DistSum : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DistSum));

		private static readonly string Name = typeof(DistSum).Name;

		private static readonly string NParts = "mapreduce.pi." + Name + ".nParts";

		/// <summary>DistSum job parameters</summary>
		internal class Parameters
		{
			internal const int Count = 6;

			internal const string List = "<nThreads> <nJobs> <type> <nPart> <remoteDir> <localDir>";

			internal const string Description = "\n  <nThreads> The number of working threads."
				 + "\n  <nJobs> The number of jobs per sum." + "\n  <type> 'm' for map side job, 'r' for reduce side job, 'x' for mix type."
				 + "\n  <nPart> The number of parts per job." + "\n  <remoteDir> Remote directory for submitting jobs."
				 + "\n  <localDir> Local directory for storing output files.";

			/// <summary>Number of worker threads</summary>
			internal readonly int nThreads;

			/// <summary>Number of jobs</summary>
			internal readonly int nJobs;

			/// <summary>Number of parts per job</summary>
			internal readonly int nParts;

			/// <summary>The machine used in the computation</summary>
			internal readonly DistSum.Machine machine;

			/// <summary>The remote job directory</summary>
			internal readonly string remoteDir;

			/// <summary>The local output directory</summary>
			internal readonly FilePath localDir;

			private Parameters(DistSum.Machine machine, int nThreads, int nJobs, int nParts, 
				string remoteDir, FilePath localDir)
			{
				/////////////////////////////////////////////////////////////////////////////
				this.machine = machine;
				this.nThreads = nThreads;
				this.nJobs = nJobs;
				this.nParts = nParts;
				this.remoteDir = remoteDir;
				this.localDir = localDir;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return "\nnThreads  = " + nThreads + "\nnJobs     = " + nJobs + "\nnParts    = " 
					+ nParts + " (" + machine + ")" + "\nremoteDir = " + remoteDir + "\nlocalDir  = "
					 + localDir;
			}

			/// <summary>Parse parameters</summary>
			internal static DistSum.Parameters Parse(string[] args, int i)
			{
				if (args.Length - i < Count)
				{
					throw new ArgumentException("args.length - i < COUNT = " + Count + ", args.length="
						 + args.Length + ", i=" + i + ", args=" + Arrays.AsList(args));
				}
				int nThreads = System.Convert.ToInt32(args[i++]);
				int nJobs = System.Convert.ToInt32(args[i++]);
				string type = args[i++];
				int nParts = System.Convert.ToInt32(args[i++]);
				string remoteDir = args[i++];
				FilePath localDir = new FilePath(args[i++]);
				if (!"m".Equals(type) && !"r".Equals(type) && !"x".Equals(type))
				{
					throw new ArgumentException("type=" + type + " is not equal to m, r or x");
				}
				else
				{
					if (nParts <= 0)
					{
						throw new ArgumentException("nParts = " + nParts + " <= 0");
					}
					else
					{
						if (nJobs <= 0)
						{
							throw new ArgumentException("nJobs = " + nJobs + " <= 0");
						}
						else
						{
							if (nThreads <= 0)
							{
								throw new ArgumentException("nThreads = " + nThreads + " <= 0");
							}
						}
					}
				}
				Org.Apache.Hadoop.Examples.PI.Util.CheckDirectory(localDir);
				return new DistSum.Parameters("m".Equals(type) ? DistSum.MapSide.Instance : "r".Equals
					(type) ? DistSum.ReduceSide.Instance : DistSum.MixMachine.Instance, nThreads, nJobs
					, nParts, remoteDir, localDir);
			}
		}

		/// <summary>Abstract machine for job execution.</summary>
		public abstract class Machine
		{
			/////////////////////////////////////////////////////////////////////////////
			/// <summary>Initialize a job</summary>
			/// <exception cref="System.IO.IOException"/>
			internal abstract void Init(Job job);

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return GetType().Name;
			}

			/// <summary>Compute sigma</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			internal static void Compute<_T0>(Summation sigma, TaskInputOutputContext<_T0> context
				)
			{
				string s;
				Log.Info(s = "sigma=" + sigma);
				context.SetStatus(s);
				long start = Runtime.CurrentTimeMillis();
				sigma.Compute();
				long duration = Runtime.CurrentTimeMillis() - start;
				TaskResult result = new TaskResult(sigma, duration);
				Log.Info(s = "result=" + result);
				context.SetStatus(s);
				context.Write(NullWritable.Get(), result);
			}

			/// <summary>Split for the summations</summary>
			public sealed class SummationSplit : InputSplit, Writable, Container<Summation>
			{
				private static readonly string[] Empty = new string[] {  };

				private Summation sigma;

				public SummationSplit()
				{
				}

				private SummationSplit(Summation sigma)
				{
					this.sigma = sigma;
				}

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				public Summation GetElement()
				{
					return sigma;
				}

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				public override long GetLength()
				{
					return 1;
				}

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				public override string[] GetLocations()
				{
					return Empty;
				}

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				/// <exception cref="System.IO.IOException"/>
				public void ReadFields(DataInput @in)
				{
					sigma = SummationWritable.Read(@in);
				}

				/// <summary>
				/// <inheritDoc/>
				/// 
				/// </summary>
				/// <exception cref="System.IO.IOException"/>
				public void Write(DataOutput @out)
				{
					new SummationWritable(sigma).Write(@out);
				}
			}

			/// <summary>An abstract InputFormat for the jobs</summary>
			public abstract class AbstractInputFormat : InputFormat<NullWritable, SummationWritable
				>
			{
				/// <summary>Specify how to read the records</summary>
				public sealed override RecordReader<NullWritable, SummationWritable> CreateRecordReader
					(InputSplit generic, TaskAttemptContext context)
				{
					DistSum.Machine.SummationSplit split = (DistSum.Machine.SummationSplit)generic;
					//return a record reader
					return new _RecordReader_214(split);
				}

				private sealed class _RecordReader_214 : RecordReader<NullWritable, SummationWritable
					>
				{
					public _RecordReader_214(DistSum.Machine.SummationSplit split)
					{
						this.split = split;
						this.done = false;
					}

					internal bool done;

					/// <summary>
					/// <inheritDoc/>
					/// 
					/// </summary>
					public override void Initialize(InputSplit split, TaskAttemptContext context)
					{
					}

					/// <summary>
					/// <inheritDoc/>
					/// 
					/// </summary>
					public override bool NextKeyValue()
					{
						return !this.done ? this.done = true : false;
					}

					/// <summary>
					/// <inheritDoc/>
					/// 
					/// </summary>
					public override NullWritable GetCurrentKey()
					{
						return NullWritable.Get();
					}

					/// <summary>
					/// <inheritDoc/>
					/// 
					/// </summary>
					public override SummationWritable GetCurrentValue()
					{
						return new SummationWritable(split.GetElement());
					}

					/// <summary>
					/// <inheritDoc/>
					/// 
					/// </summary>
					public override float GetProgress()
					{
						return this.done ? 1f : 0f;
					}

					/// <summary>
					/// <inheritDoc/>
					/// 
					/// </summary>
					public override void Close()
					{
					}

					private readonly DistSum.Machine.SummationSplit split;
				}
			}
		}

		/// <summary>A machine which does computation on the map side.</summary>
		public class MapSide : DistSum.Machine
		{
			private static readonly DistSum.MapSide Instance = new DistSum.MapSide();

			/////////////////////////////////////////////////////////////////////////////
			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			internal override void Init(Job job)
			{
				// setup mapper
				job.SetMapperClass(typeof(DistSum.MapSide.SummingMapper));
				job.SetMapOutputKeyClass(typeof(NullWritable));
				job.SetMapOutputValueClass(typeof(TaskResult));
				// zero reducer
				job.SetNumReduceTasks(0);
				// setup input
				job.SetInputFormatClass(typeof(DistSum.MapSide.PartitionInputFormat));
			}

			/// <summary>An InputFormat which partitions a summation</summary>
			public class PartitionInputFormat : DistSum.Machine.AbstractInputFormat
			{
				/// <summary>Partitions the summation into parts and then return them as splits</summary>
				public override IList<InputSplit> GetSplits(JobContext context)
				{
					//read sigma from conf
					Configuration conf = context.GetConfiguration();
					Summation sigma = SummationWritable.Read(typeof(DistSum), conf);
					int nParts = conf.GetInt(NParts, 0);
					//create splits
					IList<InputSplit> splits = new AList<InputSplit>(nParts);
					Summation[] parts = sigma.Partition(nParts);
					for (int i = 0; i < parts.Length; ++i)
					{
						splits.AddItem(new DistSum.Machine.SummationSplit(parts[i]));
					}
					//LOG.info("parts[" + i + "] = " + parts[i]);
					return splits;
				}
			}

			/// <summary>A mapper which computes sums</summary>
			public class SummingMapper : Mapper<NullWritable, SummationWritable, NullWritable
				, TaskResult>
			{
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				protected override void Map(NullWritable nw, SummationWritable sigma, Mapper.Context
					 context)
				{
					Compute(sigma.GetElement(), context);
				}
			}
		}

		/// <summary>A machine which does computation on the reduce side.</summary>
		public class ReduceSide : DistSum.Machine
		{
			private static readonly DistSum.ReduceSide Instance = new DistSum.ReduceSide();

			/////////////////////////////////////////////////////////////////////////////
			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			internal override void Init(Job job)
			{
				// setup mapper
				job.SetMapperClass(typeof(DistSum.ReduceSide.PartitionMapper));
				job.SetMapOutputKeyClass(typeof(IntWritable));
				job.SetMapOutputValueClass(typeof(SummationWritable));
				// setup partitioner
				job.SetPartitionerClass(typeof(DistSum.ReduceSide.IndexPartitioner));
				// setup reducer
				job.SetReducerClass(typeof(DistSum.ReduceSide.SummingReducer));
				job.SetOutputKeyClass(typeof(NullWritable));
				job.SetOutputValueClass(typeof(TaskResult));
				Configuration conf = job.GetConfiguration();
				int nParts = conf.GetInt(NParts, 1);
				job.SetNumReduceTasks(nParts);
				// setup input
				job.SetInputFormatClass(typeof(DistSum.ReduceSide.SummationInputFormat));
			}

			/// <summary>An InputFormat which returns a single summation.</summary>
			public class SummationInputFormat : DistSum.Machine.AbstractInputFormat
			{
				/// <returns>a list containing a single split of summation</returns>
				public override IList<InputSplit> GetSplits(JobContext context)
				{
					//read sigma from conf
					Configuration conf = context.GetConfiguration();
					Summation sigma = SummationWritable.Read(typeof(DistSum), conf);
					//create splits
					IList<InputSplit> splits = new AList<InputSplit>(1);
					splits.AddItem(new DistSum.Machine.SummationSplit(sigma));
					return splits;
				}
			}

			/// <summary>A Mapper which partitions a summation</summary>
			public class PartitionMapper : Mapper<NullWritable, SummationWritable, IntWritable
				, SummationWritable>
			{
				/// <summary>Partitions sigma into parts</summary>
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				protected override void Map(NullWritable nw, SummationWritable sigma, Mapper.Context
					 context)
				{
					Configuration conf = context.GetConfiguration();
					int nParts = conf.GetInt(NParts, 0);
					Summation[] parts = sigma.GetElement().Partition(nParts);
					for (int i = 0; i < parts.Length; ++i)
					{
						context.Write(new IntWritable(i), new SummationWritable(parts[i]));
						Log.Info("parts[" + i + "] = " + parts[i]);
					}
				}
			}

			/// <summary>Use the index for partitioning.</summary>
			public class IndexPartitioner : Partitioner<IntWritable, SummationWritable>
			{
				/// <summary>Return the index as the partition.</summary>
				public override int GetPartition(IntWritable index, SummationWritable value, int 
					numPartitions)
				{
					return index.Get();
				}
			}

			/// <summary>A Reducer which computes sums</summary>
			public class SummingReducer : Reducer<IntWritable, SummationWritable, NullWritable
				, TaskResult>
			{
				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				protected override void Reduce(IntWritable index, IEnumerable<SummationWritable> 
					sums, Reducer.Context context)
				{
					Log.Info("index=" + index);
					foreach (SummationWritable sigma in sums)
					{
						Compute(sigma.GetElement(), context);
					}
				}
			}
		}

		/// <summary>A machine which chooses Machine in runtime according to the cluster status
		/// 	</summary>
		public class MixMachine : DistSum.Machine
		{
			private static readonly DistSum.MixMachine Instance = new DistSum.MixMachine();

			private Cluster cluster;

			/////////////////////////////////////////////////////////////////////////////
			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			internal override void Init(Job job)
			{
				lock (this)
				{
					Configuration conf = job.GetConfiguration();
					if (cluster == null)
					{
						string jobTrackerStr = conf.Get("mapreduce.jobtracker.address", "localhost:8012");
						cluster = new Cluster(NetUtils.CreateSocketAddr(jobTrackerStr), conf);
					}
					ChooseMachine(conf).Init(job);
				}
			}

			/// <summary>Choose a Machine in runtime according to the cluster status.</summary>
			/// <exception cref="System.IO.IOException"/>
			private DistSum.Machine ChooseMachine(Configuration conf)
			{
				int parts = conf.GetInt(NParts, int.MaxValue);
				try
				{
					for (; ; Sharpen.Thread.Sleep(2000))
					{
						//get cluster status
						ClusterMetrics status = cluster.GetClusterStatus();
						int m = status.GetMapSlotCapacity() - status.GetOccupiedMapSlots();
						int r = status.GetReduceSlotCapacity() - status.GetOccupiedReduceSlots();
						if (m >= parts || r >= parts)
						{
							//favor ReduceSide machine
							DistSum.Machine value = r >= parts ? DistSum.ReduceSide.Instance : DistSum.MapSide
								.Instance;
							Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine("  " + this + " is " + value + 
								" (m=" + m + ", r=" + r + ")");
							return value;
						}
					}
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}
		}

		private readonly Util.Timer timer = new Util.Timer(true);

		private DistSum.Parameters parameters;

		/////////////////////////////////////////////////////////////////////////////
		/// <summary>Get Parameters</summary>
		internal DistSum.Parameters GetParameters()
		{
			return parameters;
		}

		/// <summary>Set Parameters</summary>
		internal void SetParameters(DistSum.Parameters p)
		{
			parameters = p;
		}

		/// <summary>Create a job</summary>
		/// <exception cref="System.IO.IOException"/>
		private Job CreateJob(string name, Summation sigma)
		{
			Job job = Job.GetInstance(GetConf(), parameters.remoteDir + "/" + name);
			Configuration jobconf = job.GetConfiguration();
			job.SetJarByClass(typeof(DistSum));
			jobconf.SetInt(NParts, parameters.nParts);
			SummationWritable.Write(sigma, typeof(DistSum), jobconf);
			// disable task timeout
			jobconf.SetLong(MRJobConfig.TaskTimeout, 0);
			// do not use speculative execution
			jobconf.SetBoolean(MRJobConfig.MapSpeculative, false);
			jobconf.SetBoolean(MRJobConfig.ReduceSpeculative, false);
			return job;
		}

		/// <summary>Start a job to compute sigma</summary>
		/// <exception cref="System.IO.IOException"/>
		private void Compute(string name, Summation sigma)
		{
			if (sigma.GetValue() != null)
			{
				throw new IOException("sigma.getValue() != null, sigma=" + sigma);
			}
			//setup remote directory
			FileSystem fs = FileSystem.Get(GetConf());
			Path dir = fs.MakeQualified(new Path(parameters.remoteDir, name));
			if (!Org.Apache.Hadoop.Examples.PI.Util.CreateNonexistingDirectory(fs, dir))
			{
				return;
			}
			//setup a job
			Job job = CreateJob(name, sigma);
			Path outdir = new Path(dir, "out");
			FileOutputFormat.SetOutputPath(job, outdir);
			//start a map/reduce job
			string startmessage = "steps/parts = " + sigma.E.GetSteps() + "/" + parameters.nParts
				 + " = " + Org.Apache.Hadoop.Examples.PI.Util.Long2string(sigma.E.GetSteps() / parameters
				.nParts);
			Org.Apache.Hadoop.Examples.PI.Util.RunJob(name, job, parameters.machine, startmessage
				, timer);
			IList<TaskResult> results = Org.Apache.Hadoop.Examples.PI.Util.ReadJobOutputs(fs, 
				outdir);
			Org.Apache.Hadoop.Examples.PI.Util.WriteResults(name, results, fs, parameters.remoteDir
				);
			fs.Delete(dir, true);
			//combine results
			IList<TaskResult> combined = Org.Apache.Hadoop.Examples.PI.Util.Combine(results);
			PrintWriter @out = Org.Apache.Hadoop.Examples.PI.Util.CreateWriter(parameters.localDir
				, name);
			try
			{
				foreach (TaskResult r in combined)
				{
					string s = TaskResult2string(name, r);
					@out.WriteLine(s);
					@out.Flush();
					Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine(s);
				}
			}
			finally
			{
				@out.Close();
			}
			if (combined.Count == 1)
			{
				Summation s = combined[0].GetElement();
				if (sigma.Contains(s) && s.Contains(sigma))
				{
					sigma.SetValue(s.GetValue());
				}
			}
		}

		/// <summary>Convert a TaskResult to a String</summary>
		public static string TaskResult2string(string name, TaskResult result)
		{
			return Name + " " + name + "> " + result;
		}

		/// <summary>Convert a String to a (String, TaskResult) pair</summary>
		public static KeyValuePair<string, TaskResult> String2TaskResult(string s)
		{
			//  LOG.info("line = " + line);
			int j = s.IndexOf(Name);
			if (j == 0)
			{
				int i = j + Name.Length + 1;
				j = s.IndexOf("> ", i);
				string key = Sharpen.Runtime.Substring(s, i, j);
				TaskResult value = TaskResult.ValueOf(Sharpen.Runtime.Substring(s, j + 2));
				return new _KeyValuePair_510(key, value);
			}
			return null;
		}

		private sealed class _KeyValuePair_510 : KeyValuePair<string, TaskResult>
		{
			public _KeyValuePair_510(string key, TaskResult value)
			{
				this.key = key;
				this.value = value;
			}

			public string Key
			{
				get
				{
					return key;
				}
			}

			public TaskResult Value
			{
				get
				{
					return value;
				}
			}

			public TaskResult SetValue(TaskResult value)
			{
				throw new NotSupportedException();
			}

			private readonly string key;

			private readonly TaskResult value;
		}

		/// <summary>Callable computation</summary>
		internal class Computation : Callable<DistSum.Computation>
		{
			private readonly int index;

			private readonly string name;

			private readonly Summation sigma;

			internal Computation(DistSum _enclosing, int index, string name, Summation sigma)
			{
				this._enclosing = _enclosing;
				this.index = index;
				this.name = name;
				this.sigma = sigma;
			}

			/// <returns>The job name</returns>
			internal virtual string GetJobName()
			{
				return string.Format("%s.job%03d", this.name, this.index);
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return this.GetJobName() + this.sigma;
			}

			/// <summary>Start the computation</summary>
			public virtual DistSum.Computation Call()
			{
				if (this.sigma.GetValue() == null)
				{
					try
					{
						this._enclosing.Compute(this.GetJobName(), this.sigma);
					}
					catch (Exception e)
					{
						Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine("ERROR: Got an exception from "
							 + this.GetJobName());
						Sharpen.Runtime.PrintStackTrace(e, Org.Apache.Hadoop.Examples.PI.Util.@out);
					}
				}
				return this;
			}

			private readonly DistSum _enclosing;
		}

		/// <summary>Partition sigma and execute the computations.</summary>
		private Summation Execute(string name, Summation sigma)
		{
			Summation[] summations = sigma.Partition(parameters.nJobs);
			IList<DistSum.Computation> computations = new AList<DistSum.Computation>();
			for (int i = 0; i < summations.Length; i++)
			{
				computations.AddItem(new DistSum.Computation(this, i, name, summations[i]));
			}
			try
			{
				Org.Apache.Hadoop.Examples.PI.Util.Execute(parameters.nThreads, computations);
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
			IList<Summation> combined = Org.Apache.Hadoop.Examples.PI.Util.Combine(Arrays.AsList
				(summations));
			return combined.Count == 1 ? combined[0] : null;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.Exception"/>
		public int Run(string[] args)
		{
			//parse arguments
			if (args.Length != DistSum.Parameters.Count + 2)
			{
				return Org.Apache.Hadoop.Examples.PI.Util.PrintUsage(args, GetType().FullName + " <name> <sigma> "
					 + DistSum.Parameters.List + "\n  <name> The name." + "\n  <sigma> The summation."
					 + DistSum.Parameters.Description);
			}
			int i = 0;
			string name = args[i++];
			Summation sigma = Summation.ValueOf(args[i++]);
			SetParameters(DistSum.Parameters.Parse(args, i));
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine();
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine("name  = " + name);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine("sigma = " + sigma);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine(parameters);
			Org.Apache.Hadoop.Examples.PI.Util.@out.WriteLine();
			//run jobs
			Summation result = Execute(name, sigma);
			if (result.Equals(sigma))
			{
				sigma.SetValue(result.GetValue());
				timer.Tick("\n\nDONE\n\nsigma=" + sigma);
				return 0;
			}
			else
			{
				timer.Tick("\n\nDONE WITH ERROR\n\nresult=" + result);
				return 1;
			}
		}

		/// <summary>main</summary>
		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			System.Environment.Exit(ToolRunner.Run(null, new DistSum(), args));
		}
	}
}
