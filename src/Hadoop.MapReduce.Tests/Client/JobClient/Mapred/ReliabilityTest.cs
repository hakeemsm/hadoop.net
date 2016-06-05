using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// This class tests reliability of the framework in the face of failures of
	/// both tasks and tasktrackers.
	/// </summary>
	/// <remarks>
	/// This class tests reliability of the framework in the face of failures of
	/// both tasks and tasktrackers. Steps:
	/// 1) Get the cluster status
	/// 2) Get the number of slots in the cluster
	/// 3) Spawn a sleepjob that occupies the entire cluster (with two waves of maps)
	/// 4) Get the list of running attempts for the job
	/// 5) Fail a few of them
	/// 6) Now fail a few trackers (ssh)
	/// 7) Job should run to completion
	/// 8) The above is repeated for the Sort suite of job (randomwriter, sort,
	/// validator). All jobs must complete, and finally, the sort validation
	/// should succeed.
	/// To run the test:
	/// ./bin/hadoop --config <config> jar
	/// build/hadoop-<version>-test.jar MRReliabilityTest -libjars
	/// build/hadoop-<version>-examples.jar [-scratchdir <dir>]"
	/// The scratchdir is optional and by default the current directory on the client
	/// will be used as the scratch space. Note that password-less SSH must be set up
	/// between the client machine from where the test is submitted, and the cluster
	/// nodes where the test runs.
	/// The test should be run on a <b>free</b> cluster where there is no other parallel
	/// job submission going on. Submission of other jobs while the test runs can cause
	/// the tests/jobs submitted to fail.
	/// </remarks>
	public class ReliabilityTest : Configured, Tool
	{
		private string dir;

		private static readonly Log Log = LogFactory.GetLog(typeof(ReliabilityTest));

		private void DisplayUsage()
		{
			Log.Info("This must be run in only the distributed mode " + "(LocalJobRunner not supported).\n\tUsage: MRReliabilityTest "
				 + "-libjars <path to hadoop-examples.jar> [-scratchdir <dir>]" + "\n[-scratchdir] points to a scratch space on this host where temp"
				 + " files for this test will be created. Defaults to current working" + " dir. \nPasswordless SSH must be set up between this host and the"
				 + " nodes which the test is going to use.\n" + "The test should be run on a free cluster with no parallel job submission"
				 + " going on, as the test requires to restart TaskTrackers and kill tasks" + " any job submission while the tests are running can cause jobs/tests to fail"
				);
			System.Environment.Exit(-1);
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			Configuration conf = GetConf();
			if ("local".Equals(conf.Get(JTConfig.JtIpcAddress, "local")))
			{
				DisplayUsage();
			}
			string[] otherArgs = new GenericOptionsParser(conf, args).GetRemainingArgs();
			if (otherArgs.Length == 2)
			{
				if (otherArgs[0].Equals("-scratchdir"))
				{
					dir = otherArgs[1];
				}
				else
				{
					DisplayUsage();
				}
			}
			else
			{
				if (otherArgs.Length == 0)
				{
					dir = Runtime.GetProperty("user.dir");
				}
				else
				{
					DisplayUsage();
				}
			}
			//to protect against the case of jobs failing even when multiple attempts
			//fail, set some high values for the max attempts
			conf.SetInt(JobContext.MapMaxAttempts, 10);
			conf.SetInt(JobContext.ReduceMaxAttempts, 10);
			RunSleepJobTest(new JobClient(new JobConf(conf)), conf);
			RunSortJobTests(new JobClient(new JobConf(conf)), conf);
			return 0;
		}

		/// <exception cref="System.Exception"/>
		private void RunSleepJobTest(JobClient jc, Configuration conf)
		{
			ClusterStatus c = jc.GetClusterStatus();
			int maxMaps = c.GetMaxMapTasks() * 2;
			int maxReduces = maxMaps;
			int mapSleepTime = (int)c.GetTTExpiryInterval();
			int reduceSleepTime = mapSleepTime;
			string[] sleepJobArgs = new string[] { "-m", Sharpen.Extensions.ToString(maxMaps)
				, "-r", Sharpen.Extensions.ToString(maxReduces), "-mt", Sharpen.Extensions.ToString
				(mapSleepTime), "-rt", Sharpen.Extensions.ToString(reduceSleepTime) };
			RunTest(jc, conf, "org.apache.hadoop.mapreduce.SleepJob", sleepJobArgs, new ReliabilityTest.KillTaskThread
				(this, jc, 2, 0.2f, false, 2), new ReliabilityTest.KillTrackerThread(this, jc, 2
				, 0.4f, false, 1));
			Log.Info("SleepJob done");
		}

		/// <exception cref="System.Exception"/>
		private void RunSortJobTests(JobClient jc, Configuration conf)
		{
			string inputPath = "my_reliability_test_input";
			string outputPath = "my_reliability_test_output";
			FileSystem fs = jc.GetFs();
			fs.Delete(new Path(inputPath), true);
			fs.Delete(new Path(outputPath), true);
			RunRandomWriterTest(jc, conf, inputPath);
			RunSortTest(jc, conf, inputPath, outputPath);
			RunSortValidatorTest(jc, conf, inputPath, outputPath);
		}

		/// <exception cref="System.Exception"/>
		private void RunRandomWriterTest(JobClient jc, Configuration conf, string inputPath
			)
		{
			RunTest(jc, conf, "org.apache.hadoop.examples.RandomWriter", new string[] { inputPath
				 }, null, new ReliabilityTest.KillTrackerThread(this, jc, 0, 0.4f, false, 1));
			Log.Info("RandomWriter job done");
		}

		/// <exception cref="System.Exception"/>
		private void RunSortTest(JobClient jc, Configuration conf, string inputPath, string
			 outputPath)
		{
			RunTest(jc, conf, "org.apache.hadoop.examples.Sort", new string[] { inputPath, outputPath
				 }, new ReliabilityTest.KillTaskThread(this, jc, 2, 0.2f, false, 2), new ReliabilityTest.KillTrackerThread
				(this, jc, 2, 0.8f, false, 1));
			Log.Info("Sort job done");
		}

		/// <exception cref="System.Exception"/>
		private void RunSortValidatorTest(JobClient jc, Configuration conf, string inputPath
			, string outputPath)
		{
			RunTest(jc, conf, "org.apache.hadoop.mapred.SortValidator", new string[] { "-sortInput"
				, inputPath, "-sortOutput", outputPath }, new ReliabilityTest.KillTaskThread(this
				, jc, 2, 0.2f, false, 1), new ReliabilityTest.KillTrackerThread(this, jc, 2, 0.8f
				, false, 1));
			Log.Info("SortValidator job done");
		}

		private string NormalizeCommandPath(string command)
		{
			string hadoopHome;
			if ((hadoopHome = Runtime.Getenv("HADOOP_PREFIX")) != null)
			{
				command = hadoopHome + "/" + command;
			}
			return command;
		}

		private void CheckJobExitStatus(int status, string jobName)
		{
			if (status != 0)
			{
				Log.Info(jobName + " job failed with status: " + status);
				System.Environment.Exit(status);
			}
			else
			{
				Log.Info(jobName + " done.");
			}
		}

		//Starts the job in a thread. It also starts the taskKill/tasktrackerKill
		//threads.
		/// <exception cref="System.Exception"/>
		private void RunTest(JobClient jc, Configuration conf, string jobClass, string[] 
			args, ReliabilityTest.KillTaskThread killTaskThread, ReliabilityTest.KillTrackerThread
			 killTrackerThread)
		{
			Sharpen.Thread t = new _Thread_202(this, conf, jobClass, args, "Job Test");
			t.SetDaemon(true);
			t.Start();
			JobStatus[] jobs;
			//get the job ID. This is the job that we just submitted
			while ((jobs = jc.JobsToComplete()).Length == 0)
			{
				Log.Info("Waiting for the job " + jobClass + " to start");
				Sharpen.Thread.Sleep(1000);
			}
			JobID jobId = ((JobID)jobs[jobs.Length - 1].GetJobID());
			RunningJob rJob = jc.GetJob(jobId);
			if (rJob.IsComplete())
			{
				Log.Error("The last job returned by the querying JobTracker is complete :" + rJob
					.GetJobID() + " .Exiting the test");
				System.Environment.Exit(-1);
			}
			while (rJob.GetJobState() == JobStatus.Prep)
			{
				Log.Info("JobID : " + jobId + " not started RUNNING yet");
				Sharpen.Thread.Sleep(1000);
				rJob = jc.GetJob(jobId);
			}
			if (killTaskThread != null)
			{
				killTaskThread.SetRunningJob(rJob);
				killTaskThread.Start();
				killTaskThread.Join();
				Log.Info("DONE WITH THE TASK KILL/FAIL TESTS");
			}
			if (killTrackerThread != null)
			{
				killTrackerThread.SetRunningJob(rJob);
				killTrackerThread.Start();
				killTrackerThread.Join();
				Log.Info("DONE WITH THE TESTS TO DO WITH LOST TASKTRACKERS");
			}
			t.Join();
		}

		private sealed class _Thread_202 : Sharpen.Thread
		{
			public _Thread_202(ReliabilityTest _enclosing, Configuration conf, string jobClass
				, string[] args, string baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.jobClass = jobClass;
				this.args = args;
			}

			public override void Run()
			{
				try
				{
					Type jobClassObj = conf.GetClassByName(jobClass);
					int status = ToolRunner.Run(conf, (Tool)(System.Activator.CreateInstance(jobClassObj
						)), args);
					this._enclosing.CheckJobExitStatus(status, jobClass);
				}
				catch (Exception)
				{
					ReliabilityTest.Log.Fatal("JOB " + jobClass + " failed to run");
					System.Environment.Exit(-1);
				}
			}

			private readonly ReliabilityTest _enclosing;

			private readonly Configuration conf;

			private readonly string jobClass;

			private readonly string[] args;
		}

		private class KillTrackerThread : Sharpen.Thread
		{
			private volatile bool killed = false;

			private JobClient jc;

			private RunningJob rJob;

			private readonly int thresholdMultiplier;

			private float threshold = 0.2f;

			private bool onlyMapsProgress;

			private int numIterations;

			private readonly string slavesFile = this._enclosing.dir + "/_reliability_test_slaves_file_";

			internal readonly string shellCommand = this._enclosing.NormalizeCommandPath("bin/slaves.sh"
				);

			private readonly string StopCommand = "ps uwwx | grep java | grep " + "org.apache.hadoop.mapred.TaskTracker"
				 + " |" + " grep -v grep | tr -s ' ' | cut -d ' ' -f2 | xargs kill -s STOP";

			private readonly string ResumeCommand = "ps uwwx | grep java | grep " + "org.apache.hadoop.mapred.TaskTracker"
				 + " |" + " grep -v grep | tr -s ' ' | cut -d ' ' -f2 | xargs kill -s CONT";

			public KillTrackerThread(ReliabilityTest _enclosing, JobClient jc, int threshaldMultiplier
				, float threshold, bool onlyMapsProgress, int numIterations)
			{
				this._enclosing = _enclosing;
				//Only one instance must be active at any point
				this.jc = jc;
				this.thresholdMultiplier = threshaldMultiplier;
				this.threshold = threshold;
				this.onlyMapsProgress = onlyMapsProgress;
				this.numIterations = numIterations;
				this.SetDaemon(true);
			}

			public virtual void SetRunningJob(RunningJob rJob)
			{
				this.rJob = rJob;
			}

			public virtual void Kill()
			{
				this.killed = true;
			}

			public override void Run()
			{
				this.StopStartTrackers(true);
				if (!this.onlyMapsProgress)
				{
					this.StopStartTrackers(false);
				}
			}

			private void StopStartTrackers(bool considerMaps)
			{
				if (considerMaps)
				{
					ReliabilityTest.Log.Info("Will STOP/RESUME tasktrackers based on Maps'" + " progress"
						);
				}
				else
				{
					ReliabilityTest.Log.Info("Will STOP/RESUME tasktrackers based on " + "Reduces' progress"
						);
				}
				ReliabilityTest.Log.Info("Initial progress threshold: " + this.threshold + ". Threshold Multiplier: "
					 + this.thresholdMultiplier + ". Number of iterations: " + this.numIterations);
				float thresholdVal = this.threshold;
				int numIterationsDone = 0;
				while (!this.killed)
				{
					try
					{
						float progress;
						if (this.jc.GetJob(this.rJob.GetID()).IsComplete() || numIterationsDone == this.numIterations)
						{
							break;
						}
						if (considerMaps)
						{
							progress = this.jc.GetJob(this.rJob.GetID()).MapProgress();
						}
						else
						{
							progress = this.jc.GetJob(this.rJob.GetID()).ReduceProgress();
						}
						if (progress >= thresholdVal)
						{
							numIterationsDone++;
							ClusterStatus c;
							this.StopTaskTrackers((c = this.jc.GetClusterStatus(true)));
							Sharpen.Thread.Sleep((int)Math.Ceil(1.5 * c.GetTTExpiryInterval()));
							this.StartTaskTrackers();
							thresholdVal = thresholdVal * this.thresholdMultiplier;
						}
						Sharpen.Thread.Sleep(5000);
					}
					catch (Exception)
					{
						this.killed = true;
						return;
					}
					catch (Exception e)
					{
						ReliabilityTest.Log.Fatal(StringUtils.StringifyException(e));
					}
				}
			}

			/// <exception cref="System.Exception"/>
			private void StopTaskTrackers(ClusterStatus c)
			{
				ICollection<string> trackerNames = c.GetActiveTrackerNames();
				AList<string> trackerNamesList = new AList<string>(trackerNames);
				Sharpen.Collections.Shuffle(trackerNamesList);
				int count = 0;
				FileOutputStream fos = new FileOutputStream(new FilePath(this.slavesFile));
				ReliabilityTest.Log.Info(new DateTime() + " Stopping a few trackers");
				foreach (string tracker in trackerNamesList)
				{
					string host = this.ConvertTrackerNameToHostName(tracker);
					ReliabilityTest.Log.Info(new DateTime() + " Marking tracker on host: " + host);
					fos.Write(Sharpen.Runtime.GetBytesForString((host + "\n")));
					if (count++ >= trackerNamesList.Count / 2)
					{
						break;
					}
				}
				fos.Close();
				this.RunOperationOnTT("suspend");
			}

			/// <exception cref="System.Exception"/>
			private void StartTaskTrackers()
			{
				ReliabilityTest.Log.Info(new DateTime() + " Resuming the stopped trackers");
				this.RunOperationOnTT("resume");
				new FilePath(this.slavesFile).Delete();
			}

			/// <exception cref="System.IO.IOException"/>
			private void RunOperationOnTT(string operation)
			{
				IDictionary<string, string> hMap = new Dictionary<string, string>();
				hMap["HADOOP_SLAVES"] = this.slavesFile;
				StringTokenizer strToken;
				if (operation.Equals("suspend"))
				{
					strToken = new StringTokenizer(this.StopCommand, " ");
				}
				else
				{
					strToken = new StringTokenizer(this.ResumeCommand, " ");
				}
				string[] commandArgs = new string[strToken.CountTokens() + 1];
				int i = 0;
				commandArgs[i++] = this.shellCommand;
				while (strToken.HasMoreTokens())
				{
					commandArgs[i++] = strToken.NextToken();
				}
				string output = Shell.ExecCommand(hMap, commandArgs);
				if (output != null && !output.Equals(string.Empty))
				{
					ReliabilityTest.Log.Info(output);
				}
			}

			private string ConvertTrackerNameToHostName(string trackerName)
			{
				// Convert the trackerName to it's host name
				int indexOfColon = trackerName.IndexOf(":");
				string trackerHostName = (indexOfColon == -1) ? trackerName : Sharpen.Runtime.Substring
					(trackerName, 0, indexOfColon);
				return Sharpen.Runtime.Substring(trackerHostName, "tracker_".Length);
			}

			private readonly ReliabilityTest _enclosing;
		}

		private class KillTaskThread : Sharpen.Thread
		{
			private volatile bool killed = false;

			private RunningJob rJob;

			private JobClient jc;

			private readonly int thresholdMultiplier;

			private float threshold = 0.2f;

			private bool onlyMapsProgress;

			private int numIterations;

			public KillTaskThread(ReliabilityTest _enclosing, JobClient jc, int thresholdMultiplier
				, float threshold, bool onlyMapsProgress, int numIterations)
			{
				this._enclosing = _enclosing;
				this.jc = jc;
				this.thresholdMultiplier = thresholdMultiplier;
				this.threshold = threshold;
				this.onlyMapsProgress = onlyMapsProgress;
				this.numIterations = numIterations;
				this.SetDaemon(true);
			}

			public virtual void SetRunningJob(RunningJob rJob)
			{
				this.rJob = rJob;
			}

			public virtual void Kill()
			{
				this.killed = true;
			}

			public override void Run()
			{
				this.KillBasedOnProgress(true);
				if (!this.onlyMapsProgress)
				{
					this.KillBasedOnProgress(false);
				}
			}

			private void KillBasedOnProgress(bool considerMaps)
			{
				bool fail = false;
				if (considerMaps)
				{
					ReliabilityTest.Log.Info("Will kill tasks based on Maps' progress");
				}
				else
				{
					ReliabilityTest.Log.Info("Will kill tasks based on Reduces' progress");
				}
				ReliabilityTest.Log.Info("Initial progress threshold: " + this.threshold + ". Threshold Multiplier: "
					 + this.thresholdMultiplier + ". Number of iterations: " + this.numIterations);
				float thresholdVal = this.threshold;
				int numIterationsDone = 0;
				while (!this.killed)
				{
					try
					{
						float progress;
						if (this.jc.GetJob(this.rJob.GetID()).IsComplete() || numIterationsDone == this.numIterations)
						{
							break;
						}
						if (considerMaps)
						{
							progress = this.jc.GetJob(this.rJob.GetID()).MapProgress();
						}
						else
						{
							progress = this.jc.GetJob(this.rJob.GetID()).ReduceProgress();
						}
						if (progress >= thresholdVal)
						{
							numIterationsDone++;
							if (numIterationsDone > 0 && numIterationsDone % 2 == 0)
							{
								fail = true;
							}
							//fail tasks instead of kill
							ClusterStatus c = this.jc.GetClusterStatus();
							ReliabilityTest.Log.Info(new DateTime() + " Killing a few tasks");
							ICollection<TaskAttemptID> runningTasks = new AList<TaskAttemptID>();
							TaskReport[] mapReports = this.jc.GetMapTaskReports(this.rJob.GetID());
							foreach (TaskReport mapReport in mapReports)
							{
								if (mapReport.GetCurrentStatus() == TIPStatus.Running)
								{
									Sharpen.Collections.AddAll(runningTasks, mapReport.GetRunningTaskAttempts());
								}
							}
							if (runningTasks.Count > c.GetTaskTrackers() / 2)
							{
								int count = 0;
								foreach (TaskAttemptID t in runningTasks)
								{
									ReliabilityTest.Log.Info(new DateTime() + " Killed task : " + t);
									this.rJob.KillTask(t, fail);
									if (count++ > runningTasks.Count / 2)
									{
										//kill 50%
										break;
									}
								}
							}
							runningTasks.Clear();
							TaskReport[] reduceReports = this.jc.GetReduceTaskReports(this.rJob.GetID());
							foreach (TaskReport reduceReport in reduceReports)
							{
								if (reduceReport.GetCurrentStatus() == TIPStatus.Running)
								{
									Sharpen.Collections.AddAll(runningTasks, reduceReport.GetRunningTaskAttempts());
								}
							}
							if (runningTasks.Count > c.GetTaskTrackers() / 2)
							{
								int count = 0;
								foreach (TaskAttemptID t in runningTasks)
								{
									ReliabilityTest.Log.Info(new DateTime() + " Killed task : " + t);
									this.rJob.KillTask(t, fail);
									if (count++ > runningTasks.Count / 2)
									{
										//kill 50%
										break;
									}
								}
							}
							thresholdVal = thresholdVal * this.thresholdMultiplier;
						}
						Sharpen.Thread.Sleep(5000);
					}
					catch (Exception)
					{
						this.killed = true;
					}
					catch (Exception e)
					{
						ReliabilityTest.Log.Fatal(StringUtils.StringifyException(e));
					}
				}
			}

			private readonly ReliabilityTest _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int res = ToolRunner.Run(new Configuration(), new ReliabilityTest(), args);
			System.Environment.Exit(res);
		}
	}
}
