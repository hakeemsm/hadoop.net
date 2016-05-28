using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Job History Log Analyzer.</summary>
	/// <remarks>
	/// Job History Log Analyzer.
	/// <h3>Description.</h3>
	/// This a tool for parsing and analyzing history logs of map-reduce jobs.
	/// History logs contain information about execution of jobs, tasks, and
	/// attempts. This tool focuses on submission, launch, start, and finish times,
	/// as well as the success or failure of jobs, tasks, and attempts.
	/// <p>
	/// The analyzer calculates <em>per hour slot utilization</em> for the cluster
	/// as follows.
	/// For each task attempt it divides the time segment from the start of the
	/// attempt t<sub>S</sub> to the finish t<sub>F</sub> into whole hours
	/// [t<sub>0</sub>, ..., t<sub>n</sub>], where t<sub>0</sub> <= t&lt;sub>S</sub>
	/// is the maximal whole hour preceding t<sub>S</sub>, and
	/// t<sub>n</sub> &gt;= t<sub>F</sub> is the minimal whole hour after t<sub>F</sub>.
	/// Thus, [t<sub>0</sub>, ..., t<sub>n</sub>] covers the segment
	/// [t<sub>S</sub>, t<sub>F</sub>], during which the attempt was executed.
	/// Each interval [t<sub>i</sub>, t<sub>i+1</sub>] fully contained in
	/// [t<sub>S</sub>, t<sub>F</sub>] corresponds to exactly one slot on
	/// a map-reduce cluster (usually MAP-slot or REDUCE-slot).
	/// If interval [t<sub>i</sub>, t<sub>i+1</sub>] only intersects with
	/// [t<sub>S</sub>, t<sub>F</sub>] then we say that the task
	/// attempt used just a fraction of the slot during this hour.
	/// The fraction equals the size of the intersection.
	/// Let slotTime(A, h) denote the number of slots calculated that way for a
	/// specific attempt A during hour h.
	/// The tool then sums all slots for all attempts for every hour.
	/// The result is the slot hour utilization of the cluster:
	/// <tt>slotTime(h) = SUM<sub>A</sub> slotTime(A,h)</tt>.
	/// <p>
	/// Log analyzer calculates slot hours for <em>MAP</em> and <em>REDUCE</em>
	/// attempts separately.
	/// <p>
	/// Log analyzer distinguishes between <em>successful</em> and <em>failed</em>
	/// attempts. Task attempt is considered successful if its own status is SUCCESS
	/// and the statuses of the task and the job it is a part of are also SUCCESS.
	/// Otherwise the task attempt is considered failed.
	/// <p>
	/// Map-reduce clusters are usually configured to have a fixed number of MAP
	/// and REDUCE slots per node. Thus the maximal possible number of slots on
	/// the cluster is <tt>total_slots = total_nodes * slots_per_node</tt>.
	/// Effective slot hour cannot exceed <tt>total_slots</tt> for successful
	/// attempts.
	/// <p>
	/// <em>Pending time</em> characterizes the wait time of attempts.
	/// It is calculated similarly to the slot hour except that the wait interval
	/// starts when the job is submitted and ends when an attempt starts execution.
	/// In addition to that pending time also includes intervals between attempts
	/// of the same task if it was re-executed.
	/// <p>
	/// History log analyzer calculates two pending time variations. First is based
	/// on job submission time as described above, second, starts the wait interval
	/// when the job is launched rather than submitted.
	/// <h3>Input.</h3>
	/// The following input parameters can be specified in the argument string
	/// to the job log analyzer:
	/// <ul>
	/// <li><tt>-historyDir inputDir</tt> specifies the location of the directory
	/// where analyzer will be looking for job history log files.</li>
	/// <li><tt>-resFile resultFile</tt> the name of the result file.</li>
	/// <li><tt>-usersIncluded | -usersExcluded userList</tt> slot utilization and
	/// pending time can be calculated for all or for all but the specified users.
	/// <br />
	/// <tt>userList</tt> is a comma or semicolon separated list of users.</li>
	/// <li><tt>-gzip</tt> is used if history log files are compressed.
	/// Only
	/// <see cref="Org.Apache.Hadoop.IO.Compress.GzipCodec"/>
	/// is currently supported.</li>
	/// <li><tt>-jobDelimiter pattern</tt> one can concatenate original log files into
	/// larger file(s) with the specified delimiter to recognize the end of the log
	/// for one job from the next one.<br />
	/// <tt>pattern</tt> is a java regular expression
	/// <see cref="Sharpen.Pattern"/>
	/// , which should match only the log delimiters.
	/// <br />
	/// E.g. pattern <tt>".!!FILE=.*!!"</tt> matches delimiters, which contain
	/// the original history log file names in the following form:<br />
	/// <tt>"$!!FILE=my.job.tracker.com_myJobId_user_wordcount.log!!"</tt></li>
	/// <li><tt>-clean</tt> cleans up default directories used by the analyzer.</li>
	/// <li><tt>-test</tt> test one file locally and exit;
	/// does not require map-reduce.</li>
	/// <li><tt>-help</tt> print usage.</li>
	/// </ul>
	/// <h3>Output.</h3>
	/// The output file is formatted as a tab separated table consisting of four
	/// columns: <tt>SERIES, PERIOD, TYPE, SLOT_HOUR</tt>.
	/// <ul>
	/// <li><tt>SERIES</tt> one of the four statistical series;</li>
	/// <li><tt>PERIOD</tt> the start of the time interval in the following format:
	/// <tt>"yyyy-mm-dd hh:mm:ss"</tt>;</li>
	/// <li><tt>TYPE</tt> the slot type, e.g. MAP or REDUCE;</li>
	/// <li><tt>SLOT_HOUR</tt> the value of the slot usage during this
	/// time interval.</li>
	/// </ul>
	/// </remarks>
	public class JHLogAnalyzer
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JHLogAnalyzer));

		private static readonly string JhlaRootDir = Runtime.GetProperty("test.build.data"
			, "stats/JHLA");

		private static readonly Path InputDir = new Path(JhlaRootDir, "jhla_input");

		private const string BaseInputFileName = "jhla_in_";

		private static readonly Path OutputDir = new Path(JhlaRootDir, "jhla_output");

		private static readonly Path ResultFile = new Path(JhlaRootDir, "jhla_result.txt"
			);

		private static readonly Path DefaultHistoryDir = new Path("history");

		private const int DefaultTimeIntervalMsec = 1000 * 60 * 60;

		static JHLogAnalyzer()
		{
			// Constants
			// 1 hour
			Configuration.AddDefaultResource("hdfs-default.xml");
			Configuration.AddDefaultResource("hdfs-site.xml");
		}

		[System.Serializable]
		internal sealed class StatSeries
		{
			public static readonly JHLogAnalyzer.StatSeries StatAllSlotTime = new JHLogAnalyzer.StatSeries
				(AccumulatingReducer.ValueTypeLong + "allSlotTime");

			public static readonly JHLogAnalyzer.StatSeries StatFailedSlotTime = new JHLogAnalyzer.StatSeries
				(AccumulatingReducer.ValueTypeLong + "failedSlotTime");

			public static readonly JHLogAnalyzer.StatSeries StatSubmitPendingSlotTime = new JHLogAnalyzer.StatSeries
				(AccumulatingReducer.ValueTypeLong + "submitPendingSlotTime");

			public static readonly JHLogAnalyzer.StatSeries StatLaunchedPendingSlotTime = new 
				JHLogAnalyzer.StatSeries(AccumulatingReducer.ValueTypeLong + "launchedPendingSlotTime"
				);

			private string statName = null;

			private StatSeries(string name)
			{
				this.statName = name;
			}

			public override string ToString()
			{
				return JHLogAnalyzer.StatSeries.statName;
			}
		}

		private class FileCreateDaemon : Sharpen.Thread
		{
			private const int NumCreateThreads = 10;

			private static volatile int numFinishedThreads;

			private static volatile int numRunningThreads;

			private static FileStatus[] jhLogFiles;

			internal FileSystem fs;

			internal int start;

			internal int end;

			internal FileCreateDaemon(FileSystem fs, int start, int end)
			{
				this.fs = fs;
				this.start = start;
				this.end = end;
			}

			public override void Run()
			{
				try
				{
					for (int i = start; i < end; i++)
					{
						string name = GetFileName(i);
						Path controlFile = new Path(InputDir, "in_file_" + name);
						SequenceFile.Writer writer = null;
						try
						{
							writer = SequenceFile.CreateWriter(fs, fs.GetConf(), controlFile, typeof(Text), typeof(
								LongWritable), SequenceFile.CompressionType.None);
							string logFile = jhLogFiles[i].GetPath().ToString();
							writer.Append(new Text(logFile), new LongWritable(0));
						}
						catch (Exception e)
						{
							throw new IOException(e);
						}
						finally
						{
							if (writer != null)
							{
								writer.Close();
							}
							writer = null;
						}
					}
				}
				catch (IOException ex)
				{
					Log.Error("FileCreateDaemon failed.", ex);
				}
				numFinishedThreads++;
			}

			/// <exception cref="System.IO.IOException"/>
			private static void CreateControlFile(FileSystem fs, Path jhLogDir)
			{
				fs.Delete(InputDir, true);
				jhLogFiles = fs.ListStatus(jhLogDir);
				numFinishedThreads = 0;
				try
				{
					int start = 0;
					int step = jhLogFiles.Length / NumCreateThreads + ((jhLogFiles.Length % NumCreateThreads
						) > 0 ? 1 : 0);
					JHLogAnalyzer.FileCreateDaemon[] daemons = new JHLogAnalyzer.FileCreateDaemon[NumCreateThreads
						];
					numRunningThreads = 0;
					for (int tIdx = 0; tIdx < NumCreateThreads && start < jhLogFiles.Length; tIdx++)
					{
						int end = Math.Min(start + step, jhLogFiles.Length);
						daemons[tIdx] = new JHLogAnalyzer.FileCreateDaemon(fs, start, end);
						start += step;
						numRunningThreads++;
					}
					for (int tIdx_1 = 0; tIdx_1 < numRunningThreads; tIdx_1++)
					{
						daemons[tIdx_1].Start();
					}
				}
				finally
				{
					int prevValue = 0;
					while (numFinishedThreads < numRunningThreads)
					{
						if (prevValue < numFinishedThreads)
						{
							Log.Info("Finished " + numFinishedThreads + " threads out of " + numRunningThreads
								);
							prevValue = numFinishedThreads;
						}
						try
						{
							Sharpen.Thread.Sleep(500);
						}
						catch (Exception)
						{
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateControlFile(FileSystem fs, Path jhLogDir)
		{
			Log.Info("creating control file: JH log dir = " + jhLogDir);
			JHLogAnalyzer.FileCreateDaemon.CreateControlFile(fs, jhLogDir);
			Log.Info("created control file: JH log dir = " + jhLogDir);
		}

		private static string GetFileName(int fIdx)
		{
			return BaseInputFileName + Sharpen.Extensions.ToString(fIdx);
		}

		/// <summary>If keyVal is of the form KEY="VALUE", then this will return [KEY, VALUE]
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		private static string[] GetKeyValue(string t)
		{
			string[] keyVal = t.Split("=\"*|\"");
			return keyVal;
		}

		/// <summary>JobHistory log record.</summary>
		private class JobHistoryLog
		{
			internal string Jobid;

			internal string JobStatus;

			internal long SubmitTime;

			internal long LaunchTime;

			internal long FinishTime;

			internal long TotalMaps;

			internal long TotalReduces;

			internal long FinishedMaps;

			internal long FinishedReduces;

			internal string User;

			internal IDictionary<string, JHLogAnalyzer.TaskHistoryLog> tasks;

			internal virtual bool IsSuccessful()
			{
				return (JobStatus != null) && JobStatus.Equals("SUCCESS");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void ParseLine(string line)
			{
				StringTokenizer tokens = new StringTokenizer(line);
				if (!tokens.HasMoreTokens())
				{
					return;
				}
				string what = tokens.NextToken();
				// Line should start with one of the following:
				// Job, Task, MapAttempt, ReduceAttempt
				if (what.Equals("Job"))
				{
					UpdateJob(tokens);
				}
				else
				{
					if (what.Equals("Task"))
					{
						UpdateTask(tokens);
					}
					else
					{
						if (what.IndexOf("Attempt") >= 0)
						{
							UpdateTaskAttempt(tokens);
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void UpdateJob(StringTokenizer tokens)
			{
				while (tokens.HasMoreTokens())
				{
					string t = tokens.NextToken();
					string[] keyVal = GetKeyValue(t);
					if (keyVal.Length < 2)
					{
						continue;
					}
					if (keyVal[0].Equals("JOBID"))
					{
						if (Jobid == null)
						{
							Jobid = new string(keyVal[1]);
						}
						else
						{
							if (!Jobid.Equals(keyVal[1]))
							{
								Log.Error("Incorrect JOBID: " + Sharpen.Runtime.Substring(keyVal[1], 0, Math.Min(
									keyVal[1].Length, 100)) + " expect " + Jobid);
								return;
							}
						}
					}
					else
					{
						if (keyVal[0].Equals("JOB_STATUS"))
						{
							JobStatus = new string(keyVal[1]);
						}
						else
						{
							if (keyVal[0].Equals("SUBMIT_TIME"))
							{
								SubmitTime = long.Parse(keyVal[1]);
							}
							else
							{
								if (keyVal[0].Equals("LAUNCH_TIME"))
								{
									LaunchTime = long.Parse(keyVal[1]);
								}
								else
								{
									if (keyVal[0].Equals("FINISH_TIME"))
									{
										FinishTime = long.Parse(keyVal[1]);
									}
									else
									{
										if (keyVal[0].Equals("TOTAL_MAPS"))
										{
											TotalMaps = long.Parse(keyVal[1]);
										}
										else
										{
											if (keyVal[0].Equals("TOTAL_REDUCES"))
											{
												TotalReduces = long.Parse(keyVal[1]);
											}
											else
											{
												if (keyVal[0].Equals("FINISHED_MAPS"))
												{
													FinishedMaps = long.Parse(keyVal[1]);
												}
												else
												{
													if (keyVal[0].Equals("FINISHED_REDUCES"))
													{
														FinishedReduces = long.Parse(keyVal[1]);
													}
													else
													{
														if (keyVal[0].Equals("USER"))
														{
															User = new string(keyVal[1]);
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void UpdateTask(StringTokenizer tokens)
			{
				// unpack
				JHLogAnalyzer.TaskHistoryLog task = new JHLogAnalyzer.TaskHistoryLog().Parse(tokens
					);
				if (task.Taskid == null)
				{
					Log.Error("TASKID = NULL for job " + Jobid);
					return;
				}
				// update or insert
				if (tasks == null)
				{
					tasks = new Dictionary<string, JHLogAnalyzer.TaskHistoryLog>((int)(TotalMaps + TotalReduces
						));
				}
				JHLogAnalyzer.TaskHistoryLog existing = tasks[task.Taskid];
				if (existing == null)
				{
					tasks[task.Taskid] = task;
				}
				else
				{
					existing.UpdateWith(task);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void UpdateTaskAttempt(StringTokenizer tokens)
			{
				// unpack
				JHLogAnalyzer.TaskAttemptHistoryLog attempt = new JHLogAnalyzer.TaskAttemptHistoryLog
					();
				string taskID = attempt.Parse(tokens);
				if (taskID == null)
				{
					return;
				}
				if (tasks == null)
				{
					tasks = new Dictionary<string, JHLogAnalyzer.TaskHistoryLog>((int)(TotalMaps + TotalReduces
						));
				}
				JHLogAnalyzer.TaskHistoryLog existing = tasks[taskID];
				if (existing == null)
				{
					existing = new JHLogAnalyzer.TaskHistoryLog(taskID);
					tasks[taskID] = existing;
				}
				existing.UpdateWith(attempt);
			}
		}

		/// <summary>TaskHistory log record.</summary>
		private class TaskHistoryLog
		{
			internal string Taskid;

			internal string TaskType;

			internal string TaskStatus;

			internal long StartTime;

			internal long FinishTime;

			internal IDictionary<string, JHLogAnalyzer.TaskAttemptHistoryLog> attempts;

			internal TaskHistoryLog()
			{
			}

			internal TaskHistoryLog(string taskID)
			{
				// MAP, REDUCE, SETUP, CLEANUP
				Taskid = taskID;
			}

			internal virtual bool IsSuccessful()
			{
				return (TaskStatus != null) && TaskStatus.Equals("SUCCESS");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual JHLogAnalyzer.TaskHistoryLog Parse(StringTokenizer tokens)
			{
				while (tokens.HasMoreTokens())
				{
					string t = tokens.NextToken();
					string[] keyVal = GetKeyValue(t);
					if (keyVal.Length < 2)
					{
						continue;
					}
					if (keyVal[0].Equals("TASKID"))
					{
						if (Taskid == null)
						{
							Taskid = new string(keyVal[1]);
						}
						else
						{
							if (!Taskid.Equals(keyVal[1]))
							{
								Log.Error("Incorrect TASKID: " + Sharpen.Runtime.Substring(keyVal[1], 0, Math.Min
									(keyVal[1].Length, 100)) + " expect " + Taskid);
								continue;
							}
						}
					}
					else
					{
						if (keyVal[0].Equals("TASK_TYPE"))
						{
							TaskType = new string(keyVal[1]);
						}
						else
						{
							if (keyVal[0].Equals("TASK_STATUS"))
							{
								TaskStatus = new string(keyVal[1]);
							}
							else
							{
								if (keyVal[0].Equals("START_TIME"))
								{
									StartTime = long.Parse(keyVal[1]);
								}
								else
								{
									if (keyVal[0].Equals("FINISH_TIME"))
									{
										FinishTime = long.Parse(keyVal[1]);
									}
								}
							}
						}
					}
				}
				return this;
			}

			/// <summary>Update with non-null fields of the same task log record.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void UpdateWith(JHLogAnalyzer.TaskHistoryLog from)
			{
				if (Taskid == null)
				{
					Taskid = from.Taskid;
				}
				else
				{
					if (!Taskid.Equals(from.Taskid))
					{
						throw new IOException("Incorrect TASKID: " + from.Taskid + " expect " + Taskid);
					}
				}
				if (TaskType == null)
				{
					TaskType = from.TaskType;
				}
				else
				{
					if (!TaskType.Equals(from.TaskType))
					{
						Log.Error("Incorrect TASK_TYPE: " + from.TaskType + " expect " + TaskType + " for task "
							 + Taskid);
						return;
					}
				}
				if (from.TaskStatus != null)
				{
					TaskStatus = from.TaskStatus;
				}
				if (from.StartTime > 0)
				{
					StartTime = from.StartTime;
				}
				if (from.FinishTime > 0)
				{
					FinishTime = from.FinishTime;
				}
			}

			/// <summary>Update with non-null fields of the task attempt log record.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void UpdateWith(JHLogAnalyzer.TaskAttemptHistoryLog attempt)
			{
				if (attempt.TaskAttemptId == null)
				{
					Log.Error("Unexpected TASK_ATTEMPT_ID = null for task " + Taskid);
					return;
				}
				if (attempts == null)
				{
					attempts = new Dictionary<string, JHLogAnalyzer.TaskAttemptHistoryLog>();
				}
				JHLogAnalyzer.TaskAttemptHistoryLog existing = attempts[attempt.TaskAttemptId];
				if (existing == null)
				{
					attempts[attempt.TaskAttemptId] = attempt;
				}
				else
				{
					existing.UpdateWith(attempt);
				}
				// update task start time
				if (attempt.StartTime > 0 && (this.StartTime == 0 || this.StartTime > attempt.StartTime
					))
				{
					StartTime = attempt.StartTime;
				}
			}
		}

		/// <summary>TaskAttemptHistory log record.</summary>
		private class TaskAttemptHistoryLog
		{
			internal string TaskAttemptId;

			internal string TaskStatus;

			internal long StartTime;

			internal long FinishTime;

			internal long HdfsBytesRead;

			internal long HdfsBytesWritten;

			internal long FileBytesRead;

			internal long FileBytesWritten;

			// this task attempt status
			/// <summary>
			/// Task attempt is considered successful iff all three statuses
			/// of the attempt, the task, and the job equal "SUCCESS".
			/// </summary>
			internal virtual bool IsSuccessful()
			{
				return (TaskStatus != null) && TaskStatus.Equals("SUCCESS");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual string Parse(StringTokenizer tokens)
			{
				string taskID = null;
				while (tokens.HasMoreTokens())
				{
					string t = tokens.NextToken();
					string[] keyVal = GetKeyValue(t);
					if (keyVal.Length < 2)
					{
						continue;
					}
					if (keyVal[0].Equals("TASKID"))
					{
						if (taskID == null)
						{
							taskID = new string(keyVal[1]);
						}
						else
						{
							if (!taskID.Equals(keyVal[1]))
							{
								Log.Error("Incorrect TASKID: " + keyVal[1] + " expect " + taskID);
								continue;
							}
						}
					}
					else
					{
						if (keyVal[0].Equals("TASK_ATTEMPT_ID"))
						{
							if (TaskAttemptId == null)
							{
								TaskAttemptId = new string(keyVal[1]);
							}
							else
							{
								if (!TaskAttemptId.Equals(keyVal[1]))
								{
									Log.Error("Incorrect TASKID: " + keyVal[1] + " expect " + taskID);
									continue;
								}
							}
						}
						else
						{
							if (keyVal[0].Equals("TASK_STATUS"))
							{
								TaskStatus = new string(keyVal[1]);
							}
							else
							{
								if (keyVal[0].Equals("START_TIME"))
								{
									StartTime = long.Parse(keyVal[1]);
								}
								else
								{
									if (keyVal[0].Equals("FINISH_TIME"))
									{
										FinishTime = long.Parse(keyVal[1]);
									}
								}
							}
						}
					}
				}
				return taskID;
			}

			/// <summary>Update with non-null fields of the same task attempt log record.</summary>
			/// <exception cref="System.IO.IOException"/>
			internal virtual void UpdateWith(JHLogAnalyzer.TaskAttemptHistoryLog from)
			{
				if (TaskAttemptId == null)
				{
					TaskAttemptId = from.TaskAttemptId;
				}
				else
				{
					if (!TaskAttemptId.Equals(from.TaskAttemptId))
					{
						throw new IOException("Incorrect TASK_ATTEMPT_ID: " + from.TaskAttemptId + " expect "
							 + TaskAttemptId);
					}
				}
				if (from.TaskStatus != null)
				{
					TaskStatus = from.TaskStatus;
				}
				if (from.StartTime > 0)
				{
					StartTime = from.StartTime;
				}
				if (from.FinishTime > 0)
				{
					FinishTime = from.FinishTime;
				}
				if (from.HdfsBytesRead > 0)
				{
					HdfsBytesRead = from.HdfsBytesRead;
				}
				if (from.HdfsBytesWritten > 0)
				{
					HdfsBytesWritten = from.HdfsBytesWritten;
				}
				if (from.FileBytesRead > 0)
				{
					FileBytesRead = from.FileBytesRead;
				}
				if (from.FileBytesWritten > 0)
				{
					FileBytesWritten = from.FileBytesWritten;
				}
			}
		}

		/// <summary>
		/// Key = statName*date-time*taskType
		/// Value = number of msec for the our
		/// </summary>
		private class IntervalKey
		{
			internal const string KeyFieldDelimiter = "*";

			internal string statName;

			internal string dateTime;

			internal string taskType;

			internal IntervalKey(string stat, long timeMSec, string taskType)
			{
				statName = stat;
				SimpleDateFormat dateF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				dateTime = dateF.Format(Sharpen.Extensions.CreateDate(timeMSec));
				this.taskType = taskType;
			}

			internal IntervalKey(string key)
			{
				StringTokenizer keyTokens = new StringTokenizer(key, KeyFieldDelimiter);
				if (!keyTokens.HasMoreTokens())
				{
					return;
				}
				statName = keyTokens.NextToken();
				if (!keyTokens.HasMoreTokens())
				{
					return;
				}
				dateTime = keyTokens.NextToken();
				if (!keyTokens.HasMoreTokens())
				{
					return;
				}
				taskType = keyTokens.NextToken();
			}

			internal virtual void SetStatName(string stat)
			{
				statName = stat;
			}

			internal virtual string GetStringKey()
			{
				return statName + KeyFieldDelimiter + dateTime + KeyFieldDelimiter + taskType;
			}

			internal virtual Text GetTextKey()
			{
				return new Text(GetStringKey());
			}

			public override string ToString()
			{
				return GetStringKey();
			}
		}

		/// <summary>Mapper class.</summary>
		private class JHLAMapper : IOMapperBase<object>
		{
			/// <summary>
			/// A line pattern, which delimits history logs of different jobs,
			/// if multiple job logs are written in the same file.
			/// </summary>
			/// <remarks>
			/// A line pattern, which delimits history logs of different jobs,
			/// if multiple job logs are written in the same file.
			/// Null value means only one job log per file is expected.
			/// The pattern should be a regular expression as in
			/// <see cref="string.Matches(string)"/>
			/// .
			/// </remarks>
			internal string jobDelimiterPattern;

			internal int maxJobDelimiterLineLength;

			/// <summary>Count only these users jobs</summary>
			internal ICollection<string> usersIncluded;

			/// <summary>Exclude jobs of the following users</summary>
			internal ICollection<string> usersExcluded;

			/// <summary>Type of compression for compressed files: gzip</summary>
			internal Type compressionClass;

			/// <exception cref="System.IO.IOException"/>
			internal JHLAMapper()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			internal JHLAMapper(Configuration conf)
			{
				Configure(new JobConf(conf));
			}

			public override void Configure(JobConf conf)
			{
				base.Configure(conf);
				usersIncluded = GetUserList(conf.Get("jhla.users.included", null));
				usersExcluded = GetUserList(conf.Get("jhla.users.excluded", null));
				string zipClassName = conf.Get("jhla.compression.class", null);
				try
				{
					compressionClass = (zipClassName == null) ? null : Sharpen.Runtime.GetType(zipClassName
						).AsSubclass<CompressionCodec>();
				}
				catch (Exception e)
				{
					throw new RuntimeException("Compression codec not found: ", e);
				}
				jobDelimiterPattern = conf.Get("jhla.job.delimiter.pattern", null);
				maxJobDelimiterLineLength = conf.GetInt("jhla.job.delimiter.length", 512);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Map(Text key, LongWritable value, OutputCollector<Text, Text
				> output, Reporter reporter)
			{
				string name = key.ToString();
				long longValue = value.Get();
				reporter.SetStatus("starting " + name + " ::host = " + hostName);
				long tStart = Runtime.CurrentTimeMillis();
				ParseLogFile(fs, new Path(name), longValue, output, reporter);
				long tEnd = Runtime.CurrentTimeMillis();
				long execTime = tEnd - tStart;
				reporter.SetStatus("finished " + name + " ::host = " + hostName + " in " + execTime
					 / 1000 + " sec.");
			}

			/// <exception cref="System.IO.IOException"/>
			internal override object DoIO(Reporter reporter, string path, long offset)
			{
				// full path of history log file 
				// starting offset within the file
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void CollectStats(OutputCollector<Text, Text> output, string name
				, long execTime, object jobObjects)
			{
			}

			private bool IsEndOfJobLog(string line)
			{
				if (jobDelimiterPattern == null)
				{
					return false;
				}
				return line.Matches(jobDelimiterPattern);
			}

			/// <summary>Collect information about one job.</summary>
			/// <param name="fs">- file system</param>
			/// <param name="filePath">- full path of a history log file</param>
			/// <param name="offset">- starting offset in the history log file</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ParseLogFile(FileSystem fs, Path filePath, long offset, OutputCollector
				<Text, Text> output, Reporter reporter)
			{
				InputStream @in = null;
				try
				{
					// open file & seek
					FSDataInputStream stm = fs.Open(filePath);
					stm.Seek(offset);
					@in = stm;
					Log.Info("Opened " + filePath);
					reporter.SetStatus("Opened " + filePath);
					// get a compression filter if specified
					if (compressionClass != null)
					{
						CompressionCodec codec = (CompressionCodec)ReflectionUtils.NewInstance(compressionClass
							, new Configuration());
						@in = codec.CreateInputStream(stm);
						Log.Info("Codec created " + filePath);
						reporter.SetStatus("Codec created " + filePath);
					}
					BufferedReader reader = new BufferedReader(new InputStreamReader(@in));
					Log.Info("Reader created " + filePath);
					// skip to the next job log start
					long processed = 0L;
					if (jobDelimiterPattern != null)
					{
						for (string line = reader.ReadLine(); line != null; line = reader.ReadLine())
						{
							if ((stm.GetPos() - processed) > 100000)
							{
								processed = stm.GetPos();
								reporter.SetStatus("Processing " + filePath + " at " + processed);
							}
							if (IsEndOfJobLog(line))
							{
								break;
							}
						}
					}
					// parse lines and update job history
					JHLogAnalyzer.JobHistoryLog jh = new JHLogAnalyzer.JobHistoryLog();
					int jobLineCount = 0;
					for (string line_1 = ReadLine(reader); line_1 != null; line_1 = ReadLine(reader))
					{
						jobLineCount++;
						if ((stm.GetPos() - processed) > 20000)
						{
							processed = stm.GetPos();
							long numTasks = (jh.tasks == null ? 0 : jh.tasks.Count);
							string txt = "Processing " + filePath + " at " + processed + " # tasks = " + numTasks;
							reporter.SetStatus(txt);
							Log.Info(txt);
						}
						if (IsEndOfJobLog(line_1))
						{
							if (jh.Jobid != null)
							{
								Log.Info("Finished parsing job: " + jh.Jobid + " line count = " + jobLineCount);
								CollectJobStats(jh, output, reporter);
								Log.Info("Collected stats for job: " + jh.Jobid);
							}
							jh = new JHLogAnalyzer.JobHistoryLog();
							jobLineCount = 0;
						}
						else
						{
							jh.ParseLine(line_1);
						}
					}
					if (jh.Jobid == null)
					{
						Log.Error("JOBID = NULL in " + filePath + " at " + processed);
						return;
					}
					CollectJobStats(jh, output, reporter);
				}
				catch (Exception ie)
				{
					// parsing errors can happen if the file has been truncated
					Log.Error("JHLAMapper.parseLogFile", ie);
					reporter.SetStatus("JHLAMapper.parseLogFile failed " + StringUtils.StringifyException
						(ie));
					throw new IOException("Job failed.", ie);
				}
				finally
				{
					if (@in != null)
					{
						@in.Close();
					}
				}
			}

			/// <summary>Read lines until one ends with a " ." or "\" "</summary>
			private StringBuilder resBuffer = new StringBuilder();

			/// <exception cref="System.IO.IOException"/>
			private string ReadLine(BufferedReader reader)
			{
				resBuffer.Length = 0;
				reader.Mark(maxJobDelimiterLineLength);
				for (string line = reader.ReadLine(); line != null; line = reader.ReadLine())
				{
					if (IsEndOfJobLog(line))
					{
						if (resBuffer.Length == 0)
						{
							resBuffer.Append(line);
						}
						else
						{
							reader.Reset();
						}
						break;
					}
					if (resBuffer.Length == 0)
					{
						resBuffer.Append(line);
					}
					else
					{
						if (resBuffer.Length < 32000)
						{
							resBuffer.Append(line);
						}
					}
					if (line.EndsWith(" .") || line.EndsWith("\" "))
					{
						break;
					}
					reader.Mark(maxJobDelimiterLineLength);
				}
				string result = resBuffer.Length == 0 ? null : resBuffer.ToString();
				resBuffer.Length = 0;
				return result;
			}

			/// <exception cref="System.IO.IOException"/>
			private void CollectPerIntervalStats(OutputCollector<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
				> output, long start, long finish, string taskType, params JHLogAnalyzer.StatSeries
				[] stats)
			{
				long curInterval = (start / DefaultTimeIntervalMsec) * DefaultTimeIntervalMsec;
				long curTime = start;
				long accumTime = 0;
				while (curTime < finish)
				{
					// how much of the task time belonged to current interval
					long nextInterval = curInterval + DefaultTimeIntervalMsec;
					long intervalTime = ((finish < nextInterval) ? finish : nextInterval) - curTime;
					JHLogAnalyzer.IntervalKey key = new JHLogAnalyzer.IntervalKey(string.Empty, curInterval
						, taskType);
					Org.Apache.Hadoop.IO.Text val = new Org.Apache.Hadoop.IO.Text(intervalTime.ToString
						());
					foreach (JHLogAnalyzer.StatSeries statName in stats)
					{
						key.SetStatName(statName.ToString());
						output.Collect(key.GetTextKey(), val);
					}
					curTime = curInterval = nextInterval;
					accumTime += intervalTime;
				}
				// For the pending stat speculative attempts may intersect.
				// Only one of them is considered pending.
				System.Diagnostics.Debug.Assert(accumTime == finish - start || finish < start);
			}

			/// <exception cref="System.IO.IOException"/>
			private void CollectJobStats(JHLogAnalyzer.JobHistoryLog jh, OutputCollector<Org.Apache.Hadoop.IO.Text
				, Org.Apache.Hadoop.IO.Text> output, Reporter reporter)
			{
				if (jh == null)
				{
					return;
				}
				if (jh.tasks == null)
				{
					return;
				}
				if (jh.SubmitTime <= 0)
				{
					throw new IOException("Job " + jh.Jobid + " SUBMIT_TIME = " + jh.SubmitTime);
				}
				if (usersIncluded != null && !usersIncluded.Contains(jh.User))
				{
					return;
				}
				if (usersExcluded != null && usersExcluded.Contains(jh.User))
				{
					return;
				}
				int numAttempts = 0;
				long totalTime = 0;
				bool jobSuccess = jh.IsSuccessful();
				long jobWaitTime = jh.LaunchTime - jh.SubmitTime;
				// attemptSubmitTime is the job's SUBMIT_TIME,
				// or the previous attempt FINISH_TIME for all subsequent attempts
				foreach (JHLogAnalyzer.TaskHistoryLog th in jh.tasks.Values)
				{
					if (th.attempts == null)
					{
						continue;
					}
					// Task is successful iff both the task and the job are a "SUCCESS"
					long attemptSubmitTime = jh.LaunchTime;
					bool taskSuccess = jobSuccess && th.IsSuccessful();
					foreach (JHLogAnalyzer.TaskAttemptHistoryLog tah in th.attempts.Values)
					{
						// Task attempt is considered successful iff all three statuses
						// of the attempt, the task, and the job equal "SUCCESS"
						bool success = taskSuccess && tah.IsSuccessful();
						if (tah.StartTime == 0)
						{
							Log.Error("Start time 0 for task attempt " + tah.TaskAttemptId);
							continue;
						}
						if (tah.FinishTime < tah.StartTime)
						{
							Log.Error("Finish time " + tah.FinishTime + " is less than " + "Start time " + tah
								.StartTime + " for task attempt " + tah.TaskAttemptId);
							tah.FinishTime = tah.StartTime;
						}
						if (!"MAP".Equals(th.TaskType) && !"REDUCE".Equals(th.TaskType) && !"CLEANUP".Equals
							(th.TaskType) && !"SETUP".Equals(th.TaskType))
						{
							Log.Error("Unexpected TASK_TYPE = " + th.TaskType + " for attempt " + tah.TaskAttemptId
								);
						}
						CollectPerIntervalStats(output, attemptSubmitTime, tah.StartTime, th.TaskType, JHLogAnalyzer.StatSeries
							.StatLaunchedPendingSlotTime);
						CollectPerIntervalStats(output, attemptSubmitTime - jobWaitTime, tah.StartTime, th
							.TaskType, JHLogAnalyzer.StatSeries.StatSubmitPendingSlotTime);
						if (success)
						{
							CollectPerIntervalStats(output, tah.StartTime, tah.FinishTime, th.TaskType, JHLogAnalyzer.StatSeries
								.StatAllSlotTime);
						}
						else
						{
							CollectPerIntervalStats(output, tah.StartTime, tah.FinishTime, th.TaskType, JHLogAnalyzer.StatSeries
								.StatAllSlotTime, JHLogAnalyzer.StatSeries.StatFailedSlotTime);
						}
						totalTime += (tah.FinishTime - tah.StartTime);
						numAttempts++;
						if (numAttempts % 500 == 0)
						{
							reporter.SetStatus("Processing " + jh.Jobid + " at " + numAttempts);
						}
						attemptSubmitTime = tah.FinishTime;
					}
				}
				Log.Info("Total    Maps = " + jh.TotalMaps + "  Reduces = " + jh.TotalReduces);
				Log.Info("Finished Maps = " + jh.FinishedMaps + "  Reduces = " + jh.FinishedReduces
					);
				Log.Info("numAttempts = " + numAttempts);
				Log.Info("totalTime   = " + totalTime);
				Log.Info("averageAttemptTime = " + (numAttempts == 0 ? 0 : totalTime / numAttempts
					));
				Log.Info("jobTotalTime = " + (jh.FinishTime <= jh.SubmitTime ? 0 : jh.FinishTime 
					- jh.SubmitTime));
			}
		}

		public class JHLAPartitioner : Partitioner<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			>
		{
			internal const int NumReducers = 9;

			public virtual void Configure(JobConf conf)
			{
			}

			public virtual int GetPartition(Org.Apache.Hadoop.IO.Text key, Org.Apache.Hadoop.IO.Text
				 value, int numPartitions)
			{
				JHLogAnalyzer.IntervalKey intKey = new JHLogAnalyzer.IntervalKey(key.ToString());
				if (intKey.statName.Equals(JHLogAnalyzer.StatSeries.StatAllSlotTime.ToString()))
				{
					if (intKey.taskType.Equals("MAP"))
					{
						return 0;
					}
					else
					{
						if (intKey.taskType.Equals("REDUCE"))
						{
							return 1;
						}
					}
				}
				else
				{
					if (intKey.statName.Equals(JHLogAnalyzer.StatSeries.StatSubmitPendingSlotTime.ToString
						()))
					{
						if (intKey.taskType.Equals("MAP"))
						{
							return 2;
						}
						else
						{
							if (intKey.taskType.Equals("REDUCE"))
							{
								return 3;
							}
						}
					}
					else
					{
						if (intKey.statName.Equals(JHLogAnalyzer.StatSeries.StatLaunchedPendingSlotTime.ToString
							()))
						{
							if (intKey.taskType.Equals("MAP"))
							{
								return 4;
							}
							else
							{
								if (intKey.taskType.Equals("REDUCE"))
								{
									return 5;
								}
							}
						}
						else
						{
							if (intKey.statName.Equals(JHLogAnalyzer.StatSeries.StatFailedSlotTime.ToString()
								))
							{
								if (intKey.taskType.Equals("MAP"))
								{
									return 6;
								}
								else
								{
									if (intKey.taskType.Equals("REDUCE"))
									{
										return 7;
									}
								}
							}
						}
					}
				}
				return 8;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void RunJHLA(Type mapperClass, Path outputDir, Configuration fsConfig
			)
		{
			JobConf job = new JobConf(fsConfig, typeof(JHLogAnalyzer));
			job.SetPartitionerClass(typeof(JHLogAnalyzer.JHLAPartitioner));
			FileInputFormat.SetInputPaths(job, InputDir);
			job.SetInputFormat(typeof(SequenceFileInputFormat));
			job.SetMapperClass(mapperClass);
			job.SetReducerClass(typeof(AccumulatingReducer));
			FileOutputFormat.SetOutputPath(job, outputDir);
			job.SetOutputKeyClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetOutputValueClass(typeof(Org.Apache.Hadoop.IO.Text));
			job.SetNumReduceTasks(JHLogAnalyzer.JHLAPartitioner.NumReducers);
			JobClient.RunJob(job);
		}

		private class LoggingCollector : OutputCollector<Org.Apache.Hadoop.IO.Text, Org.Apache.Hadoop.IO.Text
			>
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Collect(Org.Apache.Hadoop.IO.Text key, Org.Apache.Hadoop.IO.Text
				 value)
			{
				Log.Info(key + " == " + value);
			}
		}

		/// <summary>Run job history log analyser.</summary>
		public static void Main(string[] args)
		{
			Path resFileName = ResultFile;
			Configuration conf = new Configuration();
			try
			{
				conf.SetInt("test.io.file.buffer.size", 0);
				Path historyDir = DefaultHistoryDir;
				string testFile = null;
				bool cleanup = false;
				bool initControlFiles = true;
				for (int i = 0; i < args.Length; i++)
				{
					// parse command line
					if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-historyDir"))
					{
						historyDir = new Path(args[++i]);
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-resFile"))
						{
							resFileName = new Path(args[++i]);
						}
						else
						{
							if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-usersIncluded"))
							{
								conf.Set("jhla.users.included", args[++i]);
							}
							else
							{
								if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-usersExcluded"))
								{
									conf.Set("jhla.users.excluded", args[++i]);
								}
								else
								{
									if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-gzip"))
									{
										conf.Set("jhla.compression.class", typeof(GzipCodec).GetCanonicalName());
									}
									else
									{
										if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-jobDelimiter"))
										{
											conf.Set("jhla.job.delimiter.pattern", args[++i]);
										}
										else
										{
											if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-jobDelimiterLength"))
											{
												conf.SetInt("jhla.job.delimiter.length", System.Convert.ToInt32(args[++i]));
											}
											else
											{
												if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-noInit"))
												{
													initControlFiles = false;
												}
												else
												{
													if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-test"))
													{
														testFile = args[++i];
													}
													else
													{
														if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-clean"))
														{
															cleanup = true;
														}
														else
														{
															if (Sharpen.Runtime.EqualsIgnoreCase(args[i], "-jobQueue"))
															{
																conf.Set("mapred.job.queue.name", args[++i]);
															}
															else
															{
																if (args[i].StartsWith("-Xmx"))
																{
																	conf.Set("mapred.child.java.opts", args[i]);
																}
																else
																{
																	PrintUsage();
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				if (cleanup)
				{
					Cleanup(conf);
					return;
				}
				if (testFile != null)
				{
					Log.Info("Start JHLA test ============ ");
					LocalFileSystem lfs = FileSystem.GetLocal(conf);
					conf.Set("fs.defaultFS", "file:///");
					JHLogAnalyzer.JHLAMapper map = new JHLogAnalyzer.JHLAMapper(conf);
					map.ParseLogFile(lfs, new Path(testFile), 0L, new JHLogAnalyzer.LoggingCollector(
						), Reporter.Null);
					return;
				}
				FileSystem fs = FileSystem.Get(conf);
				if (initControlFiles)
				{
					CreateControlFile(fs, historyDir);
				}
				long tStart = Runtime.CurrentTimeMillis();
				RunJHLA(typeof(JHLogAnalyzer.JHLAMapper), OutputDir, conf);
				long execTime = Runtime.CurrentTimeMillis() - tStart;
				AnalyzeResult(fs, 0, execTime, resFileName);
			}
			catch (IOException e)
			{
				System.Console.Error.Write(StringUtils.StringifyException(e));
				System.Environment.Exit(-1);
			}
		}

		private static void PrintUsage()
		{
			string className = typeof(JHLogAnalyzer).Name;
			System.Console.Error.WriteLine("Usage: " + className + "\n\t[-historyDir inputDir] | [-resFile resultFile] |"
				 + "\n\t[-usersIncluded | -usersExcluded userList] |" + "\n\t[-gzip] | [-jobDelimiter pattern] |"
				 + "\n\t[-help | -clean | -test testFile]");
			System.Environment.Exit(-1);
		}

		private static ICollection<string> GetUserList(string users)
		{
			if (users == null)
			{
				return null;
			}
			StringTokenizer tokens = new StringTokenizer(users, ",;");
			ICollection<string> userList = new AList<string>(tokens.CountTokens());
			while (tokens.HasMoreTokens())
			{
				userList.AddItem(tokens.NextToken());
			}
			return userList;
		}

		/// <summary>
		/// Result is combined from all reduce output files and is written to
		/// RESULT_FILE in the format
		/// column 1:
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static void AnalyzeResult(FileSystem fs, int testType, long execTime, Path
			 resFileName)
		{
			Log.Info("Analyzing results ...");
			DataOutputStream @out = null;
			BufferedWriter writer = null;
			try
			{
				@out = new DataOutputStream(fs.Create(resFileName));
				writer = new BufferedWriter(new OutputStreamWriter(@out));
				writer.Write("SERIES\tPERIOD\tTYPE\tSLOT_HOUR\n");
				FileStatus[] reduceFiles = fs.ListStatus(OutputDir);
				System.Diagnostics.Debug.Assert(reduceFiles.Length == JHLogAnalyzer.JHLAPartitioner
					.NumReducers);
				for (int i = 0; i < JHLogAnalyzer.JHLAPartitioner.NumReducers; i++)
				{
					DataInputStream @in = null;
					BufferedReader lines = null;
					try
					{
						@in = fs.Open(reduceFiles[i].GetPath());
						lines = new BufferedReader(new InputStreamReader(@in));
						string line;
						while ((line = lines.ReadLine()) != null)
						{
							StringTokenizer tokens = new StringTokenizer(line, "\t*");
							string attr = tokens.NextToken();
							string dateTime = tokens.NextToken();
							string taskType = tokens.NextToken();
							double val = long.Parse(tokens.NextToken()) / (double)DefaultTimeIntervalMsec;
							writer.Write(Sharpen.Runtime.Substring(attr, 2));
							// skip the stat type "l:"
							writer.Write("\t");
							writer.Write(dateTime);
							writer.Write("\t");
							writer.Write(taskType);
							writer.Write("\t");
							writer.Write((float)val.ToString());
							writer.NewLine();
						}
					}
					finally
					{
						if (lines != null)
						{
							lines.Close();
						}
						if (@in != null)
						{
							@in.Close();
						}
					}
				}
			}
			finally
			{
				if (writer != null)
				{
					writer.Close();
				}
				if (@out != null)
				{
					@out.Close();
				}
			}
			Log.Info("Analyzing results ... done.");
		}

		/// <exception cref="System.IO.IOException"/>
		private static void Cleanup(Configuration conf)
		{
			Log.Info("Cleaning up test files");
			FileSystem fs = FileSystem.Get(conf);
			fs.Delete(new Path(JhlaRootDir), true);
		}
	}
}
