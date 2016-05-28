using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A simple logger to handle the task-specific user logs.</summary>
	/// <remarks>
	/// A simple logger to handle the task-specific user logs.
	/// This class uses the system property <code>hadoop.log.dir</code>.
	/// </remarks>
	public class TaskLog
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TaskLog));

		internal const string UserlogsDirName = "userlogs";

		private static readonly FilePath LogDir = new FilePath(GetBaseLogDir(), UserlogsDirName
			).GetAbsoluteFile();

		internal static LocalFileSystem localFS = null;

		// localFS is set in (and used by) writeToIndexFile()
		public static string GetMRv2LogDir()
		{
			return Runtime.GetProperty(YarnConfiguration.YarnAppContainerLogDir);
		}

		public static FilePath GetTaskLogFile(TaskAttemptID taskid, bool isCleanup, TaskLog.LogName
			 filter)
		{
			if (GetMRv2LogDir() != null)
			{
				return new FilePath(GetMRv2LogDir(), filter.ToString());
			}
			else
			{
				return new FilePath(GetAttemptDir(taskid, isCleanup), filter.ToString());
			}
		}

		internal static FilePath GetRealTaskLogFileLocation(TaskAttemptID taskid, bool isCleanup
			, TaskLog.LogName filter)
		{
			TaskLog.LogFileDetail l;
			try
			{
				l = GetLogFileDetail(taskid, filter, isCleanup);
			}
			catch (IOException ie)
			{
				Log.Error("getTaskLogFileDetail threw an exception " + ie);
				return null;
			}
			return new FilePath(l.location, filter.ToString());
		}

		private class LogFileDetail
		{
			internal const string Location = "LOG_DIR:";

			internal string location;

			internal long start;

			internal long length;
		}

		/// <exception cref="System.IO.IOException"/>
		private static TaskLog.LogFileDetail GetLogFileDetail(TaskAttemptID taskid, TaskLog.LogName
			 filter, bool isCleanup)
		{
			FilePath indexFile = GetIndexFile(taskid, isCleanup);
			BufferedReader fis = new BufferedReader(new InputStreamReader(SecureIOUtils.OpenForRead
				(indexFile, ObtainLogDirOwner(taskid), null), Charsets.Utf8));
			//the format of the index file is
			//LOG_DIR: <the dir where the task logs are really stored>
			//stdout:<start-offset in the stdout file> <length>
			//stderr:<start-offset in the stderr file> <length>
			//syslog:<start-offset in the syslog file> <length>
			TaskLog.LogFileDetail l = new TaskLog.LogFileDetail();
			string str = null;
			try
			{
				str = fis.ReadLine();
				if (str == null)
				{
					// the file doesn't have anything
					throw new IOException("Index file for the log of " + taskid + " doesn't exist.");
				}
				l.location = Sharpen.Runtime.Substring(str, str.IndexOf(TaskLog.LogFileDetail.Location
					) + TaskLog.LogFileDetail.Location.Length);
				// special cases are the debugout and profile.out files. They are
				// guaranteed
				// to be associated with each task attempt since jvm reuse is disabled
				// when profiling/debugging is enabled
				if (filter.Equals(TaskLog.LogName.Debugout) || filter.Equals(TaskLog.LogName.Profile
					))
				{
					l.length = new FilePath(l.location, filter.ToString()).Length();
					l.start = 0;
					fis.Close();
					return l;
				}
				str = fis.ReadLine();
				while (str != null)
				{
					// look for the exact line containing the logname
					if (str.Contains(filter.ToString()))
					{
						str = Sharpen.Runtime.Substring(str, filter.ToString().Length + 1);
						string[] startAndLen = str.Split(" ");
						l.start = long.Parse(startAndLen[0]);
						l.length = long.Parse(startAndLen[1]);
						break;
					}
					str = fis.ReadLine();
				}
				fis.Close();
				fis = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, fis);
			}
			return l;
		}

		private static FilePath GetTmpIndexFile(TaskAttemptID taskid, bool isCleanup)
		{
			return new FilePath(GetAttemptDir(taskid, isCleanup), "log.tmp");
		}

		internal static FilePath GetIndexFile(TaskAttemptID taskid, bool isCleanup)
		{
			return new FilePath(GetAttemptDir(taskid, isCleanup), "log.index");
		}

		/// <summary>Obtain the owner of the log dir.</summary>
		/// <remarks>
		/// Obtain the owner of the log dir. This is
		/// determined by checking the job's log directory.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static string ObtainLogDirOwner(TaskAttemptID taskid)
		{
			Configuration conf = new Configuration();
			FileSystem raw = FileSystem.GetLocal(conf).GetRaw();
			Path jobLogDir = new Path(GetJobDir(((JobID)taskid.GetJobID())).GetAbsolutePath()
				);
			FileStatus jobStat = raw.GetFileStatus(jobLogDir);
			return jobStat.GetOwner();
		}

		internal static string GetBaseLogDir()
		{
			return Runtime.GetProperty("hadoop.log.dir");
		}

		internal static FilePath GetAttemptDir(TaskAttemptID taskid, bool isCleanup)
		{
			string cleanupSuffix = isCleanup ? ".cleanup" : string.Empty;
			return new FilePath(GetJobDir(((JobID)taskid.GetJobID())), taskid + cleanupSuffix
				);
		}

		private static long prevOutLength;

		private static long prevErrLength;

		private static long prevLogLength;

		/// <exception cref="System.IO.IOException"/>
		private static void WriteToIndexFile(string logLocation, bool isCleanup)
		{
			lock (typeof(TaskLog))
			{
				// To ensure atomicity of updates to index file, write to temporary index
				// file first and then rename.
				FilePath tmpIndexFile = GetTmpIndexFile(currentTaskid, isCleanup);
				BufferedOutputStream bos = null;
				DataOutputStream dos = null;
				try
				{
					bos = new BufferedOutputStream(SecureIOUtils.CreateForWrite(tmpIndexFile, 0x1a4));
					dos = new DataOutputStream(bos);
					//the format of the index file is
					//LOG_DIR: <the dir where the task logs are really stored>
					//STDOUT: <start-offset in the stdout file> <length>
					//STDERR: <start-offset in the stderr file> <length>
					//SYSLOG: <start-offset in the syslog file> <length>   
					dos.WriteBytes(TaskLog.LogFileDetail.Location + logLocation + "\n" + TaskLog.LogName
						.Stdout.ToString() + ":");
					dos.WriteBytes(System.Convert.ToString(prevOutLength) + " ");
					dos.WriteBytes(System.Convert.ToString(new FilePath(logLocation, TaskLog.LogName.
						Stdout.ToString()).Length() - prevOutLength) + "\n" + TaskLog.LogName.Stderr + ":"
						);
					dos.WriteBytes(System.Convert.ToString(prevErrLength) + " ");
					dos.WriteBytes(System.Convert.ToString(new FilePath(logLocation, TaskLog.LogName.
						Stderr.ToString()).Length() - prevErrLength) + "\n" + TaskLog.LogName.Syslog.ToString
						() + ":");
					dos.WriteBytes(System.Convert.ToString(prevLogLength) + " ");
					dos.WriteBytes(System.Convert.ToString(new FilePath(logLocation, TaskLog.LogName.
						Syslog.ToString()).Length() - prevLogLength) + "\n");
					dos.Close();
					dos = null;
					bos.Close();
					bos = null;
				}
				finally
				{
					IOUtils.Cleanup(Log, dos, bos);
				}
				FilePath indexFile = GetIndexFile(currentTaskid, isCleanup);
				Path indexFilePath = new Path(indexFile.GetAbsolutePath());
				Path tmpIndexFilePath = new Path(tmpIndexFile.GetAbsolutePath());
				if (localFS == null)
				{
					// set localFS once
					localFS = FileSystem.GetLocal(new Configuration());
				}
				localFS.Rename(tmpIndexFilePath, indexFilePath);
			}
		}

		private static void ResetPrevLengths(string logLocation)
		{
			prevOutLength = new FilePath(logLocation, TaskLog.LogName.Stdout.ToString()).Length
				();
			prevErrLength = new FilePath(logLocation, TaskLog.LogName.Stderr.ToString()).Length
				();
			prevLogLength = new FilePath(logLocation, TaskLog.LogName.Syslog.ToString()).Length
				();
		}

		private static volatile TaskAttemptID currentTaskid = null;

		/// <exception cref="System.IO.IOException"/>
		public static void SyncLogs(string logLocation, TaskAttemptID taskid, bool isCleanup
			)
		{
			lock (typeof(TaskLog))
			{
				System.Console.Out.Flush();
				System.Console.Error.Flush();
				Enumeration<Logger> allLoggers = LogManager.GetCurrentLoggers();
				while (allLoggers.MoveNext())
				{
					Logger l = allLoggers.Current;
					Enumeration<Appender> allAppenders = l.GetAllAppenders();
					while (allAppenders.MoveNext())
					{
						Appender a = allAppenders.Current;
						if (a is TaskLogAppender)
						{
							((TaskLogAppender)a).Flush();
						}
					}
				}
				if (currentTaskid != taskid)
				{
					currentTaskid = taskid;
					ResetPrevLengths(logLocation);
				}
				WriteToIndexFile(logLocation, isCleanup);
			}
		}

		public static void SyncLogsShutdown(ScheduledExecutorService scheduler)
		{
			lock (typeof(TaskLog))
			{
				// flush standard streams
				//
				System.Console.Out.Flush();
				System.Console.Error.Flush();
				if (scheduler != null)
				{
					scheduler.ShutdownNow();
				}
				// flush & close all appenders
				LogManager.Shutdown();
			}
		}

		public static void SyncLogs()
		{
			lock (typeof(TaskLog))
			{
				// flush standard streams
				//
				System.Console.Out.Flush();
				System.Console.Error.Flush();
				// flush flushable appenders
				//
				Logger rootLogger = Logger.GetRootLogger();
				FlushAppenders(rootLogger);
				Enumeration<Logger> allLoggers = rootLogger.GetLoggerRepository().GetCurrentLoggers
					();
				while (allLoggers.MoveNext())
				{
					Logger l = allLoggers.Current;
					FlushAppenders(l);
				}
			}
		}

		private static void FlushAppenders(Logger l)
		{
			Enumeration<Appender> allAppenders = l.GetAllAppenders();
			while (allAppenders.MoveNext())
			{
				Appender a = allAppenders.Current;
				if (a is Flushable)
				{
					try
					{
						((Flushable)a).Flush();
					}
					catch (IOException ioe)
					{
						System.Console.Error.WriteLine(a + ": Failed to flush!" + StringUtils.StringifyException
							(ioe));
					}
				}
			}
		}

		public static ScheduledExecutorService CreateLogSyncer()
		{
			ScheduledExecutorService scheduler = Executors.NewSingleThreadScheduledExecutor(new 
				_ThreadFactory_331());
			ShutdownHookManager.Get().AddShutdownHook(new _Runnable_340(scheduler), 50);
			scheduler.ScheduleWithFixedDelay(new _Runnable_347(), 0L, 5L, TimeUnit.Seconds);
			return scheduler;
		}

		private sealed class _ThreadFactory_331 : ThreadFactory
		{
			public _ThreadFactory_331()
			{
			}

			public Sharpen.Thread NewThread(Runnable r)
			{
				Sharpen.Thread t = Executors.DefaultThreadFactory().NewThread(r);
				t.SetDaemon(true);
				t.SetName("Thread for syncLogs");
				return t;
			}
		}

		private sealed class _Runnable_340 : Runnable
		{
			public _Runnable_340(ScheduledExecutorService scheduler)
			{
				this.scheduler = scheduler;
			}

			public void Run()
			{
				TaskLog.SyncLogsShutdown(scheduler);
			}

			private readonly ScheduledExecutorService scheduler;
		}

		private sealed class _Runnable_347 : Runnable
		{
			public _Runnable_347()
			{
			}

			public void Run()
			{
				TaskLog.SyncLogs();
			}
		}

		/// <summary>The filter for userlogs.</summary>
		[System.Serializable]
		public sealed class LogName
		{
			/// <summary>Log on the stdout of the task.</summary>
			public static readonly TaskLog.LogName Stdout = new TaskLog.LogName("stdout");

			/// <summary>Log on the stderr of the task.</summary>
			public static readonly TaskLog.LogName Stderr = new TaskLog.LogName("stderr");

			/// <summary>Log on the map-reduce system logs of the task.</summary>
			public static readonly TaskLog.LogName Syslog = new TaskLog.LogName("syslog");

			/// <summary>The java profiler information.</summary>
			public static readonly TaskLog.LogName Profile = new TaskLog.LogName("profile.out"
				);

			/// <summary>Log the debug script's stdout</summary>
			public static readonly TaskLog.LogName Debugout = new TaskLog.LogName("debugout");

			private string prefix;

			private LogName(string prefix)
			{
				this.prefix = prefix;
			}

			public override string ToString()
			{
				return TaskLog.LogName.prefix;
			}
		}

		public class Reader : InputStream
		{
			private long bytesRemaining;

			private FileInputStream file;

			/// <summary>Read a log file from start to end positions.</summary>
			/// <remarks>
			/// Read a log file from start to end positions. The offsets may be negative,
			/// in which case they are relative to the end of the file. For example,
			/// Reader(taskid, kind, 0, -1) is the entire file and
			/// Reader(taskid, kind, -4197, -1) is the last 4196 bytes.
			/// </remarks>
			/// <param name="taskid">the id of the task to read the log file for</param>
			/// <param name="kind">the kind of log to read</param>
			/// <param name="start">the offset to read from (negative is relative to tail)</param>
			/// <param name="end">the offset to read upto (negative is relative to tail)</param>
			/// <param name="isCleanup">whether the attempt is cleanup attempt or not</param>
			/// <exception cref="System.IO.IOException"/>
			public Reader(TaskAttemptID taskid, TaskLog.LogName kind, long start, long end, bool
				 isCleanup)
			{
				// find the right log file
				TaskLog.LogFileDetail fileDetail = GetLogFileDetail(taskid, kind, isCleanup);
				// calculate the start and stop
				long size = fileDetail.length;
				if (start < 0)
				{
					start += size + 1;
				}
				if (end < 0)
				{
					end += size + 1;
				}
				start = Math.Max(0, Math.Min(start, size));
				end = Math.Max(0, Math.Min(end, size));
				start += fileDetail.start;
				end += fileDetail.start;
				bytesRemaining = end - start;
				string owner = ObtainLogDirOwner(taskid);
				file = SecureIOUtils.OpenForRead(new FilePath(fileDetail.location, kind.ToString(
					)), owner, null);
				// skip upto start
				long pos = 0;
				while (pos < start)
				{
					long result = file.Skip(start - pos);
					if (result < 0)
					{
						bytesRemaining = 0;
						break;
					}
					pos += result;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				int result = -1;
				if (bytesRemaining > 0)
				{
					bytesRemaining -= 1;
					result = file.Read();
				}
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read(byte[] buffer, int offset, int length)
			{
				length = (int)Math.Min(length, bytesRemaining);
				int bytes = file.Read(buffer, offset, length);
				if (bytes > 0)
				{
					bytesRemaining -= bytes;
				}
				return bytes;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Available()
			{
				return (int)Math.Min(bytesRemaining, file.Available());
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				file.Close();
			}
		}

		private const string bashCommand = "bash";

		private const string tailCommand = "tail";

		/// <summary>Get the desired maximum length of task's logs.</summary>
		/// <param name="conf">the job to look in</param>
		/// <returns>the number of bytes to cap the log files at</returns>
		public static long GetTaskLogLength(JobConf conf)
		{
			return GetTaskLogLimitBytes(conf);
		}

		public static long GetTaskLogLimitBytes(Configuration conf)
		{
			return conf.GetLong(JobContext.TaskUserlogLimit, 0) * 1024;
		}

		/// <summary>Wrap a command in a shell to capture stdout and stderr to files.</summary>
		/// <remarks>
		/// Wrap a command in a shell to capture stdout and stderr to files.
		/// Setup commands such as setting memory limit can be passed which
		/// will be executed before exec.
		/// If the tailLength is 0, the entire output will be saved.
		/// </remarks>
		/// <param name="setup">The setup commands for the execed process.</param>
		/// <param name="cmd">The command and the arguments that should be run</param>
		/// <param name="stdoutFilename">The filename that stdout should be saved to</param>
		/// <param name="stderrFilename">The filename that stderr should be saved to</param>
		/// <param name="tailLength">The length of the tail to be saved.</param>
		/// <param name="useSetsid">Should setsid be used in the command or not.</param>
		/// <returns>the modified command that should be run</returns>
		/// <exception cref="System.IO.IOException"/>
		public static IList<string> CaptureOutAndError(IList<string> setup, IList<string>
			 cmd, FilePath stdoutFilename, FilePath stderrFilename, long tailLength, bool useSetsid
			)
		{
			IList<string> result = new AList<string>(3);
			result.AddItem(bashCommand);
			result.AddItem("-c");
			string mergedCmd = BuildCommandLine(setup, cmd, stdoutFilename, stderrFilename, tailLength
				, useSetsid);
			result.AddItem(mergedCmd);
			return result;
		}

		/// <summary>Construct the command line for running the task JVM</summary>
		/// <param name="setup">The setup commands for the execed process.</param>
		/// <param name="cmd">The command and the arguments that should be run</param>
		/// <param name="stdoutFilename">The filename that stdout should be saved to</param>
		/// <param name="stderrFilename">The filename that stderr should be saved to</param>
		/// <param name="tailLength">The length of the tail to be saved.</param>
		/// <returns>the command line as a String</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static string BuildCommandLine(IList<string> setup, IList<string> cmd, FilePath
			 stdoutFilename, FilePath stderrFilename, long tailLength, bool useSetsid)
		{
			string stdout = FileUtil.MakeShellPath(stdoutFilename);
			string stderr = FileUtil.MakeShellPath(stderrFilename);
			StringBuilder mergedCmd = new StringBuilder();
			// Export the pid of taskJvm to env variable JVM_PID.
			// Currently pid is not used on Windows
			if (!Shell.Windows)
			{
				mergedCmd.Append(" export JVM_PID=`echo $$` ; ");
			}
			if (setup != null && setup.Count > 0)
			{
				mergedCmd.Append(AddCommand(setup, false));
				mergedCmd.Append(";");
			}
			if (tailLength > 0)
			{
				mergedCmd.Append("(");
			}
			else
			{
				if (ProcessTree.isSetsidAvailable && useSetsid && !Shell.Windows)
				{
					mergedCmd.Append("exec setsid ");
				}
				else
				{
					mergedCmd.Append("exec ");
				}
			}
			mergedCmd.Append(AddCommand(cmd, true));
			mergedCmd.Append(" < /dev/null ");
			if (tailLength > 0)
			{
				mergedCmd.Append(" | ");
				mergedCmd.Append(tailCommand);
				mergedCmd.Append(" -c ");
				mergedCmd.Append(tailLength);
				mergedCmd.Append(" >> ");
				mergedCmd.Append(stdout);
				mergedCmd.Append(" ; exit $PIPESTATUS ) 2>&1 | ");
				mergedCmd.Append(tailCommand);
				mergedCmd.Append(" -c ");
				mergedCmd.Append(tailLength);
				mergedCmd.Append(" >> ");
				mergedCmd.Append(stderr);
				mergedCmd.Append(" ; exit $PIPESTATUS");
			}
			else
			{
				mergedCmd.Append(" 1>> ");
				mergedCmd.Append(stdout);
				mergedCmd.Append(" 2>> ");
				mergedCmd.Append(stderr);
			}
			return mergedCmd.ToString();
		}

		/// <summary>Construct the command line for running the debug script</summary>
		/// <param name="cmd">The command and the arguments that should be run</param>
		/// <param name="stdoutFilename">The filename that stdout should be saved to</param>
		/// <param name="stderrFilename">The filename that stderr should be saved to</param>
		/// <param name="tailLength">The length of the tail to be saved.</param>
		/// <returns>the command line as a String</returns>
		/// <exception cref="System.IO.IOException"/>
		internal static string BuildDebugScriptCommandLine(IList<string> cmd, string debugout
			)
		{
			StringBuilder mergedCmd = new StringBuilder();
			mergedCmd.Append("exec ");
			bool isExecutable = true;
			foreach (string s in cmd)
			{
				if (isExecutable)
				{
					// the executable name needs to be expressed as a shell path for the  
					// shell to find it.
					mergedCmd.Append(FileUtil.MakeShellPath(new FilePath(s)));
					isExecutable = false;
				}
				else
				{
					mergedCmd.Append(s);
				}
				mergedCmd.Append(" ");
			}
			mergedCmd.Append(" < /dev/null ");
			mergedCmd.Append(" >");
			mergedCmd.Append(debugout);
			mergedCmd.Append(" 2>&1 ");
			return mergedCmd.ToString();
		}

		/// <summary>
		/// Add quotes to each of the command strings and
		/// return as a single string
		/// </summary>
		/// <param name="cmd">The command to be quoted</param>
		/// <param name="isExecutable">
		/// makes shell path if the first
		/// argument is executable
		/// </param>
		/// <returns>returns The quoted string.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string AddCommand(IList<string> cmd, bool isExecutable)
		{
			StringBuilder command = new StringBuilder();
			foreach (string s in cmd)
			{
				command.Append('\'');
				if (isExecutable)
				{
					// the executable name needs to be expressed as a shell path for the  
					// shell to find it.
					command.Append(FileUtil.MakeShellPath(new FilePath(s)));
					isExecutable = false;
				}
				else
				{
					command.Append(s);
				}
				command.Append('\'');
				command.Append(" ");
			}
			return command.ToString();
		}

		/// <summary>Method to return the location of user log directory.</summary>
		/// <returns>base log directory</returns>
		internal static FilePath GetUserLogDir()
		{
			if (!LogDir.Exists())
			{
				bool b = LogDir.Mkdirs();
				if (!b)
				{
					Log.Debug("mkdirs failed. Ignoring.");
				}
			}
			return LogDir;
		}

		/// <summary>Get the user log directory for the job jobid.</summary>
		/// <param name="jobid"/>
		/// <returns>user log directory for the job</returns>
		public static FilePath GetJobDir(JobID jobid)
		{
			return new FilePath(GetUserLogDir(), jobid.ToString());
		}
	}
}
