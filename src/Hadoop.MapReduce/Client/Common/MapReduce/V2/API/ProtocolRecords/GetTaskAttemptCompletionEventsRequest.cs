using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetTaskAttemptCompletionEventsRequest
	{
		JobId GetJobId();

		int GetFromEventId();

		int GetMaxEvents();

		void SetJobId(JobId jobId);

		void SetFromEventId(int id);

		void SetMaxEvents(int maxEvents);
	}
}
