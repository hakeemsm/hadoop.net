using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Utilities used in unit test.</summary>
	public class UtilsForTests
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(UtilsForTests));

		internal const long Kb = 1024L * 1;

		internal const long Mb = 1024L * Kb;

		internal const long Gb = 1024L * Mb;

		internal const long Tb = 1024L * Gb;

		internal const long Pb = 1024L * Tb;

		internal static readonly object waitLock = new object();

		internal static DecimalFormat dfm = new DecimalFormat("####.000");

		internal static DecimalFormat ifm = new DecimalFormat("###,###,###,###,###");

		public static string Dfmt(double d)
		{
			return dfm.Format(d);
		}

		public static string Ifmt(double d)
		{
			return ifm.Format(d);
		}

		public static string FormatBytes(long numBytes)
		{
			StringBuilder buf = new StringBuilder();
			bool bDetails = true;
			double num = numBytes;
			if (numBytes < Kb)
			{
				buf.Append(numBytes + " B");
				bDetails = false;
			}
			else
			{
				if (numBytes < Mb)
				{
					buf.Append(Dfmt(num / Kb) + " KB");
				}
				else
				{
					if (numBytes < Gb)
					{
						buf.Append(Dfmt(num / Mb) + " MB");
					}
					else
					{
						if (numBytes < Tb)
						{
							buf.Append(Dfmt(num / Gb) + " GB");
						}
						else
						{
							if (numBytes < Pb)
							{
								buf.Append(Dfmt(num / Tb) + " TB");
							}
							else
							{
								buf.Append(Dfmt(num / Pb) + " PB");
							}
						}
					}
				}
			}
			if (bDetails)
			{
				buf.Append(" (" + Ifmt(numBytes) + " bytes)");
			}
			return buf.ToString();
		}

		public static string FormatBytes2(long numBytes)
		{
			StringBuilder buf = new StringBuilder();
			long u = 0;
			if (numBytes >= Tb)
			{
				u = numBytes / Tb;
				numBytes -= u * Tb;
				buf.Append(u + " TB ");
			}
			if (numBytes >= Gb)
			{
				u = numBytes / Gb;
				numBytes -= u * Gb;
				buf.Append(u + " GB ");
			}
			if (numBytes >= Mb)
			{
				u = numBytes / Mb;
				numBytes -= u * Mb;
				buf.Append(u + " MB ");
			}
			if (numBytes >= Kb)
			{
				u = numBytes / Kb;
				numBytes -= u * Kb;
				buf.Append(u + " KB ");
			}
			buf.Append(u + " B");
			//even if zero
			return buf.ToString();
		}

		internal const string regexpSpecials = "[]()?*+|.!^-\\~@";

		public static string RegexpEscape(string plain)
		{
			StringBuilder buf = new StringBuilder();
			char[] ch = plain.ToCharArray();
			int csup = ch.Length;
			for (int c = 0; c < csup; c++)
			{
				if (regexpSpecials.IndexOf(ch[c]) != -1)
				{
					buf.Append("\\");
				}
				buf.Append(ch[c]);
			}
			return buf.ToString();
		}

		public static string SafeGetCanonicalPath(FilePath f)
		{
			try
			{
				string s = f.GetCanonicalPath();
				return (s == null) ? f.ToString() : s;
			}
			catch (IOException)
			{
				return f.ToString();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static string Slurp(FilePath f)
		{
			int len = (int)f.Length();
			byte[] buf = new byte[len];
			FileInputStream @in = new FileInputStream(f);
			string contents = null;
			try
			{
				@in.Read(buf, 0, len);
				contents = Sharpen.Runtime.GetStringForBytes(buf, "UTF-8");
			}
			finally
			{
				@in.Close();
			}
			return contents;
		}

		/// <exception cref="System.IO.IOException"/>
		public static string SlurpHadoop(Path p, FileSystem fs)
		{
			int len = (int)fs.GetFileStatus(p).GetLen();
			byte[] buf = new byte[len];
			InputStream @in = fs.Open(p);
			string contents = null;
			try
			{
				@in.Read(buf, 0, len);
				contents = Sharpen.Runtime.GetStringForBytes(buf, "UTF-8");
			}
			finally
			{
				@in.Close();
			}
			return contents;
		}

		public static string Rjustify(string s, int width)
		{
			if (s == null)
			{
				s = "null";
			}
			if (width > s.Length)
			{
				s = GetSpace(width - s.Length) + s;
			}
			return s;
		}

		public static string Ljustify(string s, int width)
		{
			if (s == null)
			{
				s = "null";
			}
			if (width > s.Length)
			{
				s = s + GetSpace(width - s.Length);
			}
			return s;
		}

		internal static char[] space;

		static UtilsForTests()
		{
			space = new char[300];
			Arrays.Fill(space, '\u0020');
		}

		public static string GetSpace(int len)
		{
			if (len > space.Length)
			{
				space = new char[Math.Max(len, 2 * space.Length)];
				Arrays.Fill(space, '\u0020');
			}
			return new string(space, 0, len);
		}

		/// <summary>Gets job status from the jobtracker given the jobclient and the job id</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static JobStatus GetJobStatus(JobClient jc, JobID id)
		{
			JobStatus[] statuses = jc.GetAllJobs();
			foreach (JobStatus jobStatus in statuses)
			{
				if (((JobID)jobStatus.GetJobID()).Equals(id))
				{
					return jobStatus;
				}
			}
			return null;
		}

		/// <summary>A utility that waits for specified amount of time</summary>
		public static void WaitFor(long duration)
		{
			try
			{
				lock (waitLock)
				{
					Sharpen.Runtime.Wait(waitLock, duration);
				}
			}
			catch (Exception)
			{
			}
		}

		/// <summary>Wait for the jobtracker to be RUNNING.</summary>
		internal static void WaitForJobTracker(JobClient jobClient)
		{
			while (true)
			{
				try
				{
					ClusterStatus status = jobClient.GetClusterStatus();
					while (status.GetJobTrackerStatus() != Cluster.JobTrackerStatus.Running)
					{
						WaitFor(100);
						status = jobClient.GetClusterStatus();
					}
					break;
				}
				catch (IOException)
				{
				}
			}
		}

		// means that the jt is ready
		/// <summary>Waits until all the jobs at the jobtracker complete.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void WaitTillDone(JobClient jobClient)
		{
			// Wait for the last job to complete
			while (true)
			{
				bool shouldWait = false;
				foreach (JobStatus jobStatuses in jobClient.GetAllJobs())
				{
					if (jobStatuses.GetRunState() != JobStatus.Succeeded && jobStatuses.GetRunState()
						 != JobStatus.Failed && jobStatuses.GetRunState() != JobStatus.Killed)
					{
						shouldWait = true;
						break;
					}
				}
				if (shouldWait)
				{
					WaitFor(100);
				}
				else
				{
					break;
				}
			}
		}

		/// <summary>Configure a waiting job</summary>
		/// <exception cref="System.IO.IOException"/>
		internal static void ConfigureWaitingJobConf(JobConf jobConf, Path inDir, Path outputPath
			, int numMaps, int numRed, string jobName, string mapSignalFilename, string redSignalFilename
			)
		{
			jobConf.SetJobName(jobName);
			jobConf.SetInputFormat(typeof(SortValidator.RecordStatsChecker.NonSplitableSequenceFileInputFormat
				));
			jobConf.SetOutputFormat(typeof(SequenceFileOutputFormat));
			FileInputFormat.SetInputPaths(jobConf, inDir);
			FileOutputFormat.SetOutputPath(jobConf, outputPath);
			jobConf.SetMapperClass(typeof(UtilsForTests.HalfWaitingMapper));
			jobConf.SetReducerClass(typeof(IdentityReducer));
			jobConf.SetOutputKeyClass(typeof(BytesWritable));
			jobConf.SetOutputValueClass(typeof(BytesWritable));
			jobConf.SetInputFormat(typeof(UtilsForTests.RandomInputFormat));
			jobConf.SetNumMapTasks(numMaps);
			jobConf.SetNumReduceTasks(numRed);
			jobConf.SetJar("build/test/mapred/testjar/testjob.jar");
			jobConf.Set(GetTaskSignalParameter(true), mapSignalFilename);
			jobConf.Set(GetTaskSignalParameter(false), redSignalFilename);
		}

		/// <summary>Map is a Mapper that just waits for a file to be created on the dfs.</summary>
		/// <remarks>
		/// Map is a Mapper that just waits for a file to be created on the dfs. The
		/// file creation is a signal to the mappers and hence acts as a waiting job.
		/// </remarks>
		internal class WaitingMapper : MapReduceBase, Mapper<WritableComparable, Writable
			, WritableComparable, Writable>
		{
			internal FileSystem fs = null;

			internal Path signal;

			internal int id = 0;

			internal int totalMaps = 0;

			/// <summary>Checks if the map task needs to wait.</summary>
			/// <remarks>
			/// Checks if the map task needs to wait. By default all the maps will wait.
			/// This method needs to be overridden to make a custom waiting mapper.
			/// </remarks>
			public virtual bool ShouldWait(int id)
			{
				return true;
			}

			/// <summary>Returns a signal file on which the map task should wait.</summary>
			/// <remarks>
			/// Returns a signal file on which the map task should wait. By default all
			/// the maps wait on a single file passed as test.mapred.map.waiting.target.
			/// This method needs to be overridden to make a custom waiting mapper
			/// </remarks>
			public virtual Path GetSignalFile(int id)
			{
				return signal;
			}

			/// <summary>The waiting function.</summary>
			/// <remarks>
			/// The waiting function.  The map exits once it gets a signal. Here the
			/// signal is the file existence.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable val, OutputCollector<WritableComparable
				, Writable> output, Reporter reporter)
			{
				if (ShouldWait(id))
				{
					if (fs != null)
					{
						while (!fs.Exists(GetSignalFile(id)))
						{
							try
							{
								reporter.Progress();
								lock (this)
								{
									Sharpen.Runtime.Wait(this, 1000);
								}
							}
							catch (Exception)
							{
								// wait for 1 sec
								System.Console.Out.WriteLine("Interrupted while the map was waiting for " + " the signal."
									);
								break;
							}
						}
					}
					else
					{
						throw new IOException("Could not get the DFS!!");
					}
				}
			}

			public override void Configure(JobConf conf)
			{
				try
				{
					string taskId = conf.Get(JobContext.TaskAttemptId);
					id = System.Convert.ToInt32(taskId.Split("_")[4]);
					totalMaps = System.Convert.ToInt32(conf.Get(JobContext.NumMaps));
					fs = FileSystem.Get(conf);
					signal = new Path(conf.Get(GetTaskSignalParameter(true)));
				}
				catch (IOException)
				{
					System.Console.Out.WriteLine("Got an exception while obtaining the filesystem");
				}
			}
		}

		/// <summary>
		/// Only the later half of the maps wait for the signal while the rest
		/// complete immediately.
		/// </summary>
		internal class HalfWaitingMapper : UtilsForTests.WaitingMapper
		{
			public override bool ShouldWait(int id)
			{
				return id >= (totalMaps / 2);
			}
		}

		/// <summary>Reduce that just waits for a file to be created on the dfs.</summary>
		/// <remarks>
		/// Reduce that just waits for a file to be created on the dfs. The
		/// file creation is a signal to the reduce.
		/// </remarks>
		internal class WaitingReducer : MapReduceBase, Reducer<WritableComparable, Writable
			, WritableComparable, Writable>
		{
			internal FileSystem fs = null;

			internal Path signal;

			/// <summary>The waiting function.</summary>
			/// <remarks>
			/// The waiting function.  The reduce exits once it gets a signal. Here the
			/// signal is the file existence.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Reduce(WritableComparable key, IEnumerator<Writable> val, OutputCollector
				<WritableComparable, Writable> output, Reporter reporter)
			{
				if (fs != null)
				{
					while (!fs.Exists(signal))
					{
						try
						{
							reporter.Progress();
							lock (this)
							{
								Sharpen.Runtime.Wait(this, 1000);
							}
						}
						catch (Exception)
						{
							// wait for 1 sec
							System.Console.Out.WriteLine("Interrupted while the map was waiting for the" + " signal."
								);
							break;
						}
					}
				}
				else
				{
					throw new IOException("Could not get the DFS!!");
				}
			}

			public override void Configure(JobConf conf)
			{
				try
				{
					fs = FileSystem.Get(conf);
					signal = new Path(conf.Get(GetTaskSignalParameter(false)));
				}
				catch (IOException)
				{
					System.Console.Out.WriteLine("Got an exception while obtaining the filesystem");
				}
			}
		}

		internal static string GetTaskSignalParameter(bool isMap)
		{
			return isMap ? "test.mapred.map.waiting.target" : "test.mapred.reduce.waiting.target";
		}

		/// <summary>Signal the maps/reduces to start.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		internal static void SignalTasks(MiniDFSCluster dfs, FileSystem fileSys, string mapSignalFile
			, string reduceSignalFile, int replication)
		{
			try
			{
				WriteFile(dfs.GetNameNode(), fileSys.GetConf(), new Path(mapSignalFile), (short)replication
					);
				WriteFile(dfs.GetNameNode(), fileSys.GetConf(), new Path(reduceSignalFile), (short
					)replication);
			}
			catch (Exception)
			{
			}
		}

		// Ignore
		/// <summary>Signal the maps/reduces to start.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		internal static void SignalTasks(MiniDFSCluster dfs, FileSystem fileSys, bool isMap
			, string mapSignalFile, string reduceSignalFile)
		{
			try
			{
				//  signal the maps to complete
				WriteFile(dfs.GetNameNode(), fileSys.GetConf(), isMap ? new Path(mapSignalFile) : 
					new Path(reduceSignalFile), (short)1);
			}
			catch (Exception)
			{
			}
		}

		// Ignore
		internal static string GetSignalFile(Path dir)
		{
			return (new Path(dir, "signal")).ToString();
		}

		internal static string GetMapSignalFile(Path dir)
		{
			return (new Path(dir, "map-signal")).ToString();
		}

		internal static string GetReduceSignalFile(Path dir)
		{
			return (new Path(dir, "reduce-signal")).ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		internal static void WriteFile(NameNode namenode, Configuration conf, Path name, 
			short replication)
		{
			FileSystem fileSys = FileSystem.Get(conf);
			SequenceFile.Writer writer = SequenceFile.CreateWriter(fileSys, conf, name, typeof(
				BytesWritable), typeof(BytesWritable), SequenceFile.CompressionType.None);
			writer.Append(new BytesWritable(), new BytesWritable());
			writer.Close();
			fileSys.SetReplication(name, replication);
			DFSTestUtil.WaitReplication(fileSys, name, replication);
		}

		/// <summary>
		/// A custom input format that creates virtual inputs of a single string
		/// for each map.
		/// </summary>
		public class RandomInputFormat : InputFormat<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			>
		{
			// Input formats
			/// <exception cref="System.IO.IOException"/>
			public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
			{
				InputSplit[] result = new InputSplit[numSplits];
				Path outDir = FileOutputFormat.GetOutputPath(job);
				for (int i = 0; i < result.Length; ++i)
				{
					result[i] = new FileSplit(new Path(outDir, "dummy-split-" + i), 0, 1, (string[])null
						);
				}
				return result;
			}

			internal class RandomRecordReader : RecordReader<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
				>
			{
				internal Path name;

				public RandomRecordReader(Path p)
				{
					name = p;
				}

				public virtual bool Next(Org.Apache.Hadoop.IO.Text key, Org.Apache.Hadoop.IO.Text
					 value)
				{
					if (name != null)
					{
						key.Set(name.GetName());
						name = null;
						return true;
					}
					return false;
				}

				public virtual Org.Apache.Hadoop.IO.Text CreateKey()
				{
					return new Org.Apache.Hadoop.IO.Text();
				}

				public virtual Org.Apache.Hadoop.IO.Text CreateValue()
				{
					return new Org.Apache.Hadoop.IO.Text();
				}

				public virtual long GetPos()
				{
					return 0;
				}

				public virtual void Close()
				{
				}

				public virtual float GetProgress()
				{
					return 0.0f;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RecordReader<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text>
				 GetRecordReader(InputSplit split, JobConf job, Reporter reporter)
			{
				return new UtilsForTests.RandomInputFormat.RandomRecordReader(((FileSplit)split).
					GetPath());
			}
		}

		// Start a job and return its RunningJob object
		/// <exception cref="System.IO.IOException"/>
		internal static RunningJob RunJob(JobConf conf, Path inDir, Path outDir)
		{
			return RunJob(conf, inDir, outDir, conf.GetNumMapTasks(), conf.GetNumReduceTasks(
				));
		}

		// Start a job and return its RunningJob object
		/// <exception cref="System.IO.IOException"/>
		internal static RunningJob RunJob(JobConf conf, Path inDir, Path outDir, int numMaps
			, int numReds)
		{
			string input = "The quick brown fox\n" + "has many silly\n" + "red fox sox\n";
			// submit the job and wait for it to complete
			return RunJob(conf, inDir, outDir, numMaps, numReds, input);
		}

		// Start a job with the specified input and return its RunningJob object
		/// <exception cref="System.IO.IOException"/>
		internal static RunningJob RunJob(JobConf conf, Path inDir, Path outDir, int numMaps
			, int numReds, string input)
		{
			FileSystem fs = FileSystem.Get(conf);
			if (fs.Exists(outDir))
			{
				fs.Delete(outDir, true);
			}
			if (!fs.Exists(inDir))
			{
				fs.Mkdirs(inDir);
			}
			for (int i = 0; i < numMaps; ++i)
			{
				DataOutputStream file = fs.Create(new Path(inDir, "part-" + i));
				file.WriteBytes(input);
				file.Close();
			}
			conf.SetInputFormat(typeof(TextInputFormat));
			conf.SetOutputKeyClass(typeof(LongWritable));
			conf.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			FileInputFormat.SetInputPaths(conf, inDir);
			FileOutputFormat.SetOutputPath(conf, outDir);
			conf.SetNumMapTasks(numMaps);
			conf.SetNumReduceTasks(numReds);
			JobClient jobClient = new JobClient(conf);
			RunningJob job = jobClient.SubmitJob(conf);
			return job;
		}

		// Run a job that will be succeeded and wait until it completes
		/// <exception cref="System.IO.IOException"/>
		public static RunningJob RunJobSucceed(JobConf conf, Path inDir, Path outDir)
		{
			conf.SetJobName("test-job-succeed");
			conf.SetMapperClass(typeof(IdentityMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			RunningJob job = UtilsForTests.RunJob(conf, inDir, outDir);
			long sleepCount = 0;
			while (!job.IsComplete())
			{
				try
				{
					if (sleepCount > 300)
					{
						// 30 seconds
						throw new IOException("Job didn't finish in 30 seconds");
					}
					Sharpen.Thread.Sleep(100);
					sleepCount++;
				}
				catch (Exception)
				{
					break;
				}
			}
			return job;
		}

		// Run a job that will be failed and wait until it completes
		/// <exception cref="System.IO.IOException"/>
		public static RunningJob RunJobFail(JobConf conf, Path inDir, Path outDir)
		{
			conf.SetJobName("test-job-fail");
			conf.SetMapperClass(typeof(UtilsForTests.FailMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			conf.SetMaxMapAttempts(1);
			RunningJob job = UtilsForTests.RunJob(conf, inDir, outDir);
			long sleepCount = 0;
			while (!job.IsComplete())
			{
				try
				{
					if (sleepCount > 300)
					{
						// 30 seconds
						throw new IOException("Job didn't finish in 30 seconds");
					}
					Sharpen.Thread.Sleep(100);
					sleepCount++;
				}
				catch (Exception)
				{
					break;
				}
			}
			return job;
		}

		// Run a job that will be killed and wait until it completes
		/// <exception cref="System.IO.IOException"/>
		public static RunningJob RunJobKill(JobConf conf, Path inDir, Path outDir)
		{
			conf.SetJobName("test-job-kill");
			conf.SetMapperClass(typeof(UtilsForTests.KillMapper));
			conf.SetReducerClass(typeof(IdentityReducer));
			RunningJob job = UtilsForTests.RunJob(conf, inDir, outDir);
			long sleepCount = 0;
			while (job.GetJobState() != JobStatus.Running)
			{
				try
				{
					if (sleepCount > 300)
					{
						// 30 seconds
						throw new IOException("Job didn't finish in 30 seconds");
					}
					Sharpen.Thread.Sleep(100);
					sleepCount++;
				}
				catch (Exception)
				{
					break;
				}
			}
			job.KillJob();
			sleepCount = 0;
			while (job.CleanupProgress() == 0.0f)
			{
				try
				{
					if (sleepCount > 2000)
					{
						// 20 seconds
						throw new IOException("Job cleanup didn't start in 20 seconds");
					}
					Sharpen.Thread.Sleep(10);
					sleepCount++;
				}
				catch (Exception)
				{
					break;
				}
			}
			return job;
		}

		/// <summary>Cleans up files/dirs inline.</summary>
		/// <remarks>
		/// Cleans up files/dirs inline. CleanupQueue deletes in a separate thread
		/// asynchronously.
		/// </remarks>
		public class InlineCleanupQueue : CleanupQueue
		{
			internal IList<string> stalePaths = new AList<string>();

			public InlineCleanupQueue()
			{
			}

			// do nothing
			internal override void AddToQueue(params CleanupQueue.PathDeletionContext[] contexts
				)
			{
				// delete paths in-line
				foreach (CleanupQueue.PathDeletionContext context in contexts)
				{
					try
					{
						if (!DeletePath(context))
						{
							Log.Warn("Stale path " + context.fullPath);
							stalePaths.AddItem(context.fullPath);
						}
					}
					catch (IOException e)
					{
						Log.Warn("Caught exception while deleting path " + context.fullPath);
						Log.Info(StringUtils.StringifyException(e));
						stalePaths.AddItem(context.fullPath);
					}
				}
			}
		}

		internal class FakeClock : Clock
		{
			internal long time = 0;

			public virtual void Advance(long millis)
			{
				time += millis;
			}

			internal override long GetTime()
			{
				return time;
			}
		}

		internal class FailMapper : MapReduceBase, Mapper<WritableComparable, Writable, WritableComparable
			, Writable>
		{
			// Mapper that fails
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable value, OutputCollector<WritableComparable
				, Writable> @out, Reporter reporter)
			{
				//NOTE- the next line is required for the TestDebugScript test to succeed
				System.Console.Error.WriteLine("failing map");
				throw new RuntimeException("failing map");
			}
		}

		internal class KillMapper : MapReduceBase, Mapper<WritableComparable, Writable, WritableComparable
			, Writable>
		{
			// Mapper that sleeps for a long time.
			// Used for running a job that will be killed
			/// <exception cref="System.IO.IOException"/>
			public virtual void Map(WritableComparable key, Writable value, OutputCollector<WritableComparable
				, Writable> @out, Reporter reporter)
			{
				try
				{
					Sharpen.Thread.Sleep(1000000);
				}
				catch (Exception)
				{
				}
			}
			// Do nothing
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void SetUpConfigFile(Properties confProps, FilePath configFile)
		{
			Configuration config = new Configuration(false);
			FileOutputStream fos = new FileOutputStream(configFile);
			for (Enumeration<object> e = confProps.PropertyNames(); e.MoveNext(); )
			{
				string key = (string)e.Current;
				config.Set(key, confProps.GetProperty(key));
			}
			config.WriteXml(fos);
			fos.Close();
		}

		/// <summary>This creates a file in the dfs</summary>
		/// <param name="dfs">FileSystem Local File System where file needs to be picked</param>
		/// <param name="Uripath">Path dfs path where file needs to be copied</param>
		/// <param name="permission">FsPermission File permission</param>
		/// <returns>returns the DataOutputStream</returns>
		/// <exception cref="System.Exception"/>
		public static DataOutputStream CreateTmpFileDFS(FileSystem dfs, Path Uripath, FsPermission
			 permission, string input)
		{
			//Creating the path with the file
			DataOutputStream file = FileSystem.Create(dfs, Uripath, permission);
			file.WriteBytes(input);
			file.Close();
			return file;
		}

		/// <summary>This formats the long tasktracker name to just the FQDN</summary>
		/// <param name="taskTrackerLong">String The long format of the tasktracker string</param>
		/// <returns>String The FQDN of the tasktracker</returns>
		/// <exception cref="System.Exception"/>
		public static string GetFQDNofTT(string taskTrackerLong)
		{
			//Getting the exact FQDN of the tasktracker from the tasktracker string.
			string[] firstSplit = taskTrackerLong.Split("_");
			string tmpOutput = firstSplit[1];
			string[] secondSplit = tmpOutput.Split(":");
			string tmpTaskTracker = secondSplit[0];
			return tmpTaskTracker;
		}
	}
}
