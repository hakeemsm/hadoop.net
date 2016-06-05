using System;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	public class HostUtil
	{
		/// <summary>Construct the taskLogUrl</summary>
		/// <param name="taskTrackerHostName"/>
		/// <param name="httpPort"/>
		/// <param name="taskAttemptID"/>
		/// <returns>the taskLogUrl</returns>
		public static string GetTaskLogUrl(string scheme, string taskTrackerHostName, string
			 httpPort, string taskAttemptID)
		{
			return (scheme + taskTrackerHostName + ":" + httpPort + "/tasklog?attemptid=" + taskAttemptID
				);
		}

		/// <summary>
		/// Always throws
		/// <see cref="Sharpen.RuntimeException"/>
		/// because this method is not
		/// supposed to be called at runtime. This method is only for keeping
		/// binary compatibility with Hive 0.13. MAPREDUCE-5830 for the details.
		/// </summary>
		[System.ObsoleteAttribute(@"Use GetTaskLogUrl(string, string, string, string) to construct the taskLogUrl."
			)]
		public static string GetTaskLogUrl(string taskTrackerHostName, string httpPort, string
			 taskAttemptID)
		{
			throw new RuntimeException("This method is not supposed to be called at runtime. "
				 + "Use HostUtil.getTaskLogUrl(String, String, String, String) instead.");
		}

		public static string ConvertTrackerNameToHostName(string trackerName)
		{
			// Ugly!
			// Convert the trackerName to its host name
			int indexOfColon = trackerName.IndexOf(":");
			string trackerHostName = (indexOfColon == -1) ? trackerName : Sharpen.Runtime.Substring
				(trackerName, 0, indexOfColon);
			return Sharpen.Runtime.Substring(trackerHostName, "tracker_".Length);
		}
	}
}
