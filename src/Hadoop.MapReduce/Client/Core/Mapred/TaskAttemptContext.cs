using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public interface TaskAttemptContext : TaskAttemptContext
	{
		TaskAttemptID GetTaskAttemptID();

		Progressable GetProgressible();

		JobConf GetJobConf();
	}
}
