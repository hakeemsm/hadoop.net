using System.Collections.Generic;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords
{
	public interface GetTaskAttemptCompletionEventsResponse
	{
		IList<TaskAttemptCompletionEvent> GetCompletionEventList();

		TaskAttemptCompletionEvent GetCompletionEvent(int index);

		int GetCompletionEventCount();

		void AddAllCompletionEvents(IList<TaskAttemptCompletionEvent> eventList);

		void AddCompletionEvent(TaskAttemptCompletionEvent @event);

		void RemoveCompletionEvent(int index);

		void ClearCompletionEvents();
	}
}
