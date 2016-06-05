using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface AMInfo
	{
		ApplicationAttemptId GetAppAttemptId();

		long GetStartTime();

		ContainerId GetContainerId();

		string GetNodeManagerHost();

		int GetNodeManagerPort();

		int GetNodeManagerHttpPort();

		void SetAppAttemptId(ApplicationAttemptId appAttemptId);

		void SetStartTime(long startTime);

		void SetContainerId(ContainerId containerId);

		void SetNodeManagerHost(string nmHost);

		void SetNodeManagerPort(int nmPort);

		void SetNodeManagerHttpPort(int mnHttpPort);
	}
}
