using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public interface JobContext : JobContext
	{
		/// <summary>Get the job Configuration</summary>
		/// <returns>JobConf</returns>
		JobConf GetJobConf();

		/// <summary>Get the progress mechanism for reporting progress.</summary>
		/// <returns>progress mechanism</returns>
		Progressable GetProgressible();
	}
}
