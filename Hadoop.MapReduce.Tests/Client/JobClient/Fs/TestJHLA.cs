using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Test Job History Log Analyzer.</summary>
	/// <seealso cref="JHLogAnalyzer"/>
	public class TestJHLA : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(JHLogAnalyzer));

		private string historyLog = Runtime.GetProperty("test.build.data", "build/test/data"
			) + "/history/test.log";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		protected override void SetUp()
		{
			FilePath logFile = new FilePath(historyLog);
			if (!logFile.GetParentFile().Exists())
			{
				if (!logFile.GetParentFile().Mkdirs())
				{
					Log.Error("Cannot create dirs for history log file: " + historyLog);
				}
			}
			if (!logFile.CreateNewFile())
			{
				Log.Error("Cannot create history log file: " + historyLog);
			}
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream
				(historyLog)));
			writer.Write("$!!FILE=file1.log!!");
			writer.NewLine();
			writer.Write("Meta VERSION=\"1\" .");
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0004\" JOBNAME=\"streamjob21364.jar\" USER=\"hadoop\" SUBMIT_TIME=\"1237962008012\" JOBCONF=\"hdfs:///job_200903250600_0004/job.xml\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0004\" JOB_PRIORITY=\"NORMAL\" .");
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0004\" LAUNCH_TIME=\"1237962008712\" TOTAL_MAPS=\"2\" TOTAL_REDUCES=\"0\" JOB_STATUS=\"PREP\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000003\" TASK_TYPE=\"SETUP\" START_TIME=\"1237962008736\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0004_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000003_0\" START_TIME=\"1237962010929\" TRACKER_NAME=\"tracker_50445\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0004_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000003_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962012459\" HOSTNAME=\"host.com\" STATE_STRING=\"setup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000003\" TASK_TYPE=\"SETUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962023824\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0004\" JOB_STATUS=\"RUNNING\" .");
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1237962024049\" SPLITS=\"host1.com,host2.com,host3.com\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000001\" TASK_TYPE=\"MAP\" START_TIME=\"1237962024065\" SPLITS=\"host1.com,host2.com,host3.com\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000000_0\" START_TIME=\"1237962026157\" TRACKER_NAME=\"tracker_50524\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000000_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962041307\" HOSTNAME=\"host.com\" STATE_STRING=\"Records R/W=2681/1\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(56630)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28327)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2681)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28327)][(MAP_OUTPUT_RECORDS)(Map output records)(2681)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000000\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962054138\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(56630)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28327)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2681)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28327)][(MAP_OUTPUT_RECORDS)(Map output records)(2681)]}\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000001_0\" START_TIME=\"1237962026077\" TRACKER_NAME=\"tracker_50162\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0004_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000001_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962041030\" HOSTNAME=\"host.com\" STATE_STRING=\"Records R/W=2634/1\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(28316)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28303)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2634)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28303)][(MAP_OUTPUT_RECORDS)(Map output records)(2634)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000001\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962054187\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(28316)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(28303)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(2634)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(28303)][(MAP_OUTPUT_RECORDS)(Map output records)(2634)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000002\" TASK_TYPE=\"CLEANUP\" START_TIME=\"1237962054187\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0004_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000002_0\" START_TIME=\"1237962055578\" TRACKER_NAME=\"tracker_50162\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0004_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0004_m_000002_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962056782\" HOSTNAME=\"host.com\" STATE_STRING=\"cleanup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0004_m_000002\" TASK_TYPE=\"CLEANUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237962069193\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0004\" FINISH_TIME=\"1237962069193\" JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"2\" FINISHED_REDUCES=\"0\" FAILED_MAPS=\"0\" FAILED_REDUCES=\"0\" COUNTERS=\"{(org.apache.hadoop.mapred.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(84946)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(56630)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5315)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(56630)][(MAP_OUTPUT_RECORDS)(Map output records)(5315)]}\" ."
				);
			writer.NewLine();
			writer.Write("$!!FILE=file2.log!!");
			writer.NewLine();
			writer.Write("Meta VERSION=\"1\" .");
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0023\" JOBNAME=\"TestJob\" USER=\"hadoop2\" SUBMIT_TIME=\"1237964779799\" JOBCONF=\"hdfs:///job_200903250600_0023/job.xml\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0023\" JOB_PRIORITY=\"NORMAL\" .");
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0023\" LAUNCH_TIME=\"1237964780928\" TOTAL_MAPS=\"2\" TOTAL_REDUCES=\"0\" JOB_STATUS=\"PREP\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_r_000001\" TASK_TYPE=\"SETUP\" START_TIME=\"1237964780940\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("ReduceAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0023_r_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000001_0\" START_TIME=\"1237964720322\" TRACKER_NAME=\"tracker_3065\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("ReduceAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0023_r_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000001_0\" TASK_STATUS=\"SUCCESS\" SHUFFLE_FINISHED=\"1237964722118\" SORT_FINISHED=\"1237964722118\" FINISH_TIME=\"1237964722118\" HOSTNAME=\"host.com\" STATE_STRING=\"setup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_r_000001\" TASK_TYPE=\"SETUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964796054\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0023\" JOB_STATUS=\"RUNNING\" .");
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1237964796176\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_m_000001\" TASK_TYPE=\"MAP\" START_TIME=\"1237964796176\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000000_0\" START_TIME=\"1237964809765\" TRACKER_NAME=\"tracker_50459\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000000_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964911772\" HOSTNAME=\"host.com\" STATE_STRING=\"\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_m_000000\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964916534\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000001_0\" START_TIME=\"1237964798169\" TRACKER_NAME=\"tracker_1524\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0023_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_m_000001_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964962960\" HOSTNAME=\"host.com\" STATE_STRING=\"\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_m_000001\" TASK_TYPE=\"MAP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964976870\" COUNTERS=\"{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(500000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(5000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(5000000)][(MAP_OUTPUT_RECORDS)(Map output records)(5000000)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_r_000000\" TASK_TYPE=\"CLEANUP\" START_TIME=\"1237964976871\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("ReduceAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0023_r_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000000_0\" START_TIME=\"1237964977208\" TRACKER_NAME=\"tracker_1524\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("ReduceAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0023_r_000000\" TASK_ATTEMPT_ID=\"attempt_200903250600_0023_r_000000_0\" TASK_STATUS=\"SUCCESS\" SHUFFLE_FINISHED=\"1237964979031\" SORT_FINISHED=\"1237964979031\" FINISH_TIME=\"1237964979032\" HOSTNAME=\"host.com\" STATE_STRING=\"cleanup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0023_r_000000\" TASK_TYPE=\"CLEANUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237964991879\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0023\" FINISH_TIME=\"1237964991879\" JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"2\" FINISHED_REDUCES=\"0\" FAILED_MAPS=\"0\" FAILED_REDUCES=\"0\" COUNTERS=\"{(org.apache.hadoop.mapred.JobInProgress$Counter)(Job Counters )[(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(2)]}{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(1000000000)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(10000000)][(SPILLED_RECORDS)(Spilled Records)(0)][(MAP_INPUT_BYTES)(Map input bytes)(10000000)][(MAP_OUTPUT_RECORDS)(Map output records)(10000000)]}\" ."
				);
			writer.NewLine();
			writer.Write("$!!FILE=file3.log!!");
			writer.NewLine();
			writer.Write("Meta VERSION=\"1\" .");
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0034\" JOBNAME=\"TestJob\" USER=\"hadoop3\" SUBMIT_TIME=\"1237966370007\" JOBCONF=\"hdfs:///job_200903250600_0034/job.xml\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0034\" JOB_PRIORITY=\"NORMAL\" .");
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0034\" LAUNCH_TIME=\"1237966371076\" TOTAL_MAPS=\"2\" TOTAL_REDUCES=\"0\" JOB_STATUS=\"PREP\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0034_m_000003\" TASK_TYPE=\"SETUP\" START_TIME=\"1237966371093\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0034_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000003_0\" START_TIME=\"1237966371524\" TRACKER_NAME=\"tracker_50118\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"SETUP\" TASKID=\"task_200903250600_0034_m_000003\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000003_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237966373174\" HOSTNAME=\"host.com\" STATE_STRING=\"setup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0034_m_000003\" TASK_TYPE=\"SETUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237966386098\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0034\" JOB_STATUS=\"RUNNING\" .");
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0034_m_000000\" TASK_TYPE=\"MAP\" START_TIME=\"1237966386111\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0034_m_000001\" TASK_TYPE=\"MAP\" START_TIME=\"1237966386124\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"MAP\" TASKID=\"task_200903250600_0034_m_000001\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000001_0\" TASK_STATUS=\"FAILED\" FINISH_TIME=\"1237967174546\" HOSTNAME=\"host.com\" ERROR=\"java.io.IOException: Task process exit with nonzero status of 15."
				);
			writer.NewLine();
			writer.Write("  at org.apache.hadoop.mapred.TaskRunner.run(TaskRunner.java:424)");
			writer.NewLine();
			writer.Write(",java.io.IOException: Task process exit with nonzero status of 15."
				);
			writer.NewLine();
			writer.Write("  at org.apache.hadoop.mapred.TaskRunner.run(TaskRunner.java:424)");
			writer.NewLine();
			writer.Write("\" .");
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0034_m_000002\" TASK_TYPE=\"CLEANUP\" START_TIME=\"1237967170815\" SPLITS=\"\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0034_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000002_0\" START_TIME=\"1237967168653\" TRACKER_NAME=\"tracker_3105\" HTTP_PORT=\"50060\" ."
				);
			writer.NewLine();
			writer.Write("MapAttempt TASK_TYPE=\"CLEANUP\" TASKID=\"task_200903250600_0034_m_000002\" TASK_ATTEMPT_ID=\"attempt_200903250600_0034_m_000002_0\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237967171301\" HOSTNAME=\"host.com\" STATE_STRING=\"cleanup\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Task TASKID=\"task_200903250600_0034_m_000002\" TASK_TYPE=\"CLEANUP\" TASK_STATUS=\"SUCCESS\" FINISH_TIME=\"1237967185818\" COUNTERS=\"{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(SPILLED_RECORDS)(Spilled Records)(0)]}\" ."
				);
			writer.NewLine();
			writer.Write("Job JOBID=\"job_200903250600_0034\" FINISH_TIME=\"1237967185818\" JOB_STATUS=\"KILLED\" FINISHED_MAPS=\"0\" FINISHED_REDUCES=\"0\" ."
				);
			writer.NewLine();
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		protected override void TearDown()
		{
			FilePath logFile = new FilePath(historyLog);
			if (!logFile.Delete())
			{
				Log.Error("Cannot delete history log file: " + historyLog);
			}
			if (!logFile.GetParentFile().Delete())
			{
				Log.Error("Cannot delete history log dir: " + historyLog);
			}
		}

		/// <summary>Run log analyzer in test mode for file test.log.</summary>
		public virtual void TestJHLA()
		{
			string[] args = new string[] { "-test", historyLog, "-jobDelimiter", ".!!FILE=.*!!"
				 };
			JHLogAnalyzer.Main(args);
			args = new string[] { "-test", historyLog, "-jobDelimiter", ".!!FILE=.*!!", "-usersIncluded"
				, "hadoop,hadoop2" };
			JHLogAnalyzer.Main(args);
			args = new string[] { "-test", historyLog, "-jobDelimiter", ".!!FILE=.*!!", "-usersExcluded"
				, "hadoop,hadoop3" };
			JHLogAnalyzer.Main(args);
		}
	}
}
