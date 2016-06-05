using System.Collections.Generic;
using System.Net;
using System.Text;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class MapReduceChildJVM
	{
		private static string GetTaskLogFile(TaskLog.LogName filter)
		{
			return ApplicationConstants.LogDirExpansionVar + Path.Separator + filter.ToString
				();
		}

		private static string GetChildEnv(JobConf jobConf, bool isMap)
		{
			if (isMap)
			{
				return jobConf.Get(JobConf.MapredMapTaskEnv, jobConf.Get(JobConf.MapredTaskEnv));
			}
			return jobConf.Get(JobConf.MapredReduceTaskEnv, jobConf.Get(JobConf.MapredTaskEnv
				));
		}

		public static void SetVMEnv(IDictionary<string, string> environment, Task task)
		{
			JobConf conf = task.conf;
			// Add the env variables passed by the user
			string mapredChildEnv = GetChildEnv(conf, task.IsMapTask());
			MRApps.SetEnvFromInputString(environment, mapredChildEnv, conf);
			// Set logging level in the environment.
			// This is so that, if the child forks another "bin/hadoop" (common in
			// streaming) it will have the correct loglevel.
			environment["HADOOP_ROOT_LOGGER"] = MRApps.GetChildLogLevel(conf, task.IsMapTask(
				)) + ",console";
			// TODO: The following is useful for instance in streaming tasks. Should be
			// set in ApplicationMaster's env by the RM.
			string hadoopClientOpts = Runtime.Getenv("HADOOP_CLIENT_OPTS");
			if (hadoopClientOpts == null)
			{
				hadoopClientOpts = string.Empty;
			}
			else
			{
				hadoopClientOpts = hadoopClientOpts + " ";
			}
			environment["HADOOP_CLIENT_OPTS"] = hadoopClientOpts;
			// setEnvFromInputString above will add env variable values from
			// mapredChildEnv to existing variables. We want to overwrite
			// HADOOP_ROOT_LOGGER and HADOOP_CLIENT_OPTS if the user set it explicitly.
			IDictionary<string, string> tmpEnv = new Dictionary<string, string>();
			MRApps.SetEnvFromInputString(tmpEnv, mapredChildEnv, conf);
			string[] keys = new string[] { "HADOOP_ROOT_LOGGER", "HADOOP_CLIENT_OPTS" };
			foreach (string key in keys)
			{
				if (tmpEnv.Contains(key))
				{
					environment[key] = tmpEnv[key];
				}
			}
			// Add stdout/stderr env
			environment[MRJobConfig.StdoutLogfileEnv] = GetTaskLogFile(TaskLog.LogName.Stdout
				);
			environment[MRJobConfig.StderrLogfileEnv] = GetTaskLogFile(TaskLog.LogName.Stderr
				);
		}

		private static string GetChildJavaOpts(JobConf jobConf, bool isMapTask)
		{
			string userClasspath = string.Empty;
			string adminClasspath = string.Empty;
			if (isMapTask)
			{
				userClasspath = jobConf.Get(JobConf.MapredMapTaskJavaOpts, jobConf.Get(JobConf.MapredTaskJavaOpts
					, JobConf.DefaultMapredTaskJavaOpts));
				adminClasspath = jobConf.Get(MRJobConfig.MapredMapAdminJavaOpts, MRJobConfig.DefaultMapredAdminJavaOpts
					);
			}
			else
			{
				userClasspath = jobConf.Get(JobConf.MapredReduceTaskJavaOpts, jobConf.Get(JobConf
					.MapredTaskJavaOpts, JobConf.DefaultMapredTaskJavaOpts));
				adminClasspath = jobConf.Get(MRJobConfig.MapredReduceAdminJavaOpts, MRJobConfig.DefaultMapredAdminJavaOpts
					);
			}
			// Add admin classpath first so it can be overridden by user.
			return adminClasspath + " " + userClasspath;
		}

		public static IList<string> GetVMCommand(IPEndPoint taskAttemptListenerAddr, Task
			 task, JVMId jvmID)
		{
			TaskAttemptID attemptID = task.GetTaskID();
			JobConf conf = task.conf;
			Vector<string> vargs = new Vector<string>(8);
			vargs.AddItem(MRApps.CrossPlatformifyMREnv(task.conf, ApplicationConstants.Environment
				.JavaHome) + "/bin/java");
			// Add child (task) java-vm options.
			//
			// The following symbols if present in mapred.{map|reduce}.child.java.opts 
			// value are replaced:
			// + @taskid@ is interpolated with value of TaskID.
			// Other occurrences of @ will not be altered.
			//
			// Example with multiple arguments and substitutions, showing
			// jvm GC logging, and start of a passwordless JVM JMX agent so can
			// connect with jconsole and the likes to watch child memory, threads
			// and get thread dumps.
			//
			//  <property>
			//    <name>mapred.map.child.java.opts</name>
			//    <value>-Xmx 512M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
			//           -Dcom.sun.management.jmxremote.authenticate=false \
			//           -Dcom.sun.management.jmxremote.ssl=false \
			//    </value>
			//  </property>
			//
			//  <property>
			//    <name>mapred.reduce.child.java.opts</name>
			//    <value>-Xmx 1024M -verbose:gc -Xloggc:/tmp/@taskid@.gc \
			//           -Dcom.sun.management.jmxremote.authenticate=false \
			//           -Dcom.sun.management.jmxremote.ssl=false \
			//    </value>
			//  </property>
			//
			string javaOpts = GetChildJavaOpts(conf, task.IsMapTask());
			javaOpts = javaOpts.Replace("@taskid@", attemptID.ToString());
			string[] javaOptsSplit = javaOpts.Split(" ");
			for (int i = 0; i < javaOptsSplit.Length; i++)
			{
				vargs.AddItem(javaOptsSplit[i]);
			}
			Path childTmpDir = new Path(MRApps.CrossPlatformifyMREnv(conf, ApplicationConstants.Environment
				.Pwd), YarnConfiguration.DefaultContainerTempDir);
			vargs.AddItem("-Djava.io.tmpdir=" + childTmpDir);
			MRApps.AddLog4jSystemProperties(task, vargs, conf);
			if (conf.GetProfileEnabled())
			{
				if (conf.GetProfileTaskRange(task.IsMapTask()).IsIncluded(task.GetPartition()))
				{
					string profileParams = conf.Get(task.IsMapTask() ? MRJobConfig.TaskMapProfileParams
						 : MRJobConfig.TaskReduceProfileParams, conf.GetProfileParams());
					vargs.AddItem(string.Format(profileParams, GetTaskLogFile(TaskLog.LogName.Profile
						)));
				}
			}
			// Add main class and its arguments 
			vargs.AddItem(typeof(YarnChild).FullName);
			// main of Child
			// pass TaskAttemptListener's address
			vargs.AddItem(taskAttemptListenerAddr.Address.GetHostAddress());
			vargs.AddItem(Sharpen.Extensions.ToString(taskAttemptListenerAddr.Port));
			vargs.AddItem(attemptID.ToString());
			// pass task identifier
			// Finally add the jvmID
			vargs.AddItem(jvmID.GetId().ToString());
			vargs.AddItem("1>" + GetTaskLogFile(TaskLog.LogName.Stdout));
			vargs.AddItem("2>" + GetTaskLogFile(TaskLog.LogName.Stderr));
			// Final commmand
			StringBuilder mergedCommand = new StringBuilder();
			foreach (CharSequence str in vargs)
			{
				mergedCommand.Append(str).Append(" ");
			}
			Vector<string> vargsFinal = new Vector<string>(1);
			vargsFinal.AddItem(mergedCommand.ToString());
			return vargsFinal;
		}
	}
}
