using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public interface TaskAttemptCompletionEvent
	{
		TaskAttemptId GetAttemptId();

		TaskAttemptCompletionEventStatus GetStatus();

		string GetMapOutputServerAddress();

		int GetAttemptRunTime();

		int GetEventId();

		void SetAttemptId(TaskAttemptId taskAttemptId);

		void SetStatus(TaskAttemptCompletionEventStatus status);

		void SetMapOutputServerAddress(string address);

		void SetAttemptRunTime(int runTime);

		void SetEventId(int eventId);
	}
}
