using System;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class TestEventFetcher
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConsecutiveFetch()
		{
			int MaxEventsToFetch = 100;
			TaskAttemptID tid = new TaskAttemptID("12345", 1, TaskType.Reduce, 1, 1);
			TaskUmbilicalProtocol umbilical = Org.Mockito.Mockito.Mock<TaskUmbilicalProtocol>
				();
			Org.Mockito.Mockito.When(umbilical.GetMapCompletionEvents(Matchers.Any<JobID>(), 
				Matchers.AnyInt(), Matchers.AnyInt(), Matchers.Any<TaskAttemptID>())).ThenReturn
				(GetMockedCompletionEventsUpdate(0, 0));
			Org.Mockito.Mockito.When(umbilical.GetMapCompletionEvents(Matchers.Any<JobID>(), 
				Matchers.Eq(0), Matchers.Eq(MaxEventsToFetch), Matchers.Eq(tid))).ThenReturn(GetMockedCompletionEventsUpdate
				(0, MaxEventsToFetch));
			Org.Mockito.Mockito.When(umbilical.GetMapCompletionEvents(Matchers.Any<JobID>(), 
				Matchers.Eq(MaxEventsToFetch), Matchers.Eq(MaxEventsToFetch), Matchers.Eq(tid)))
				.ThenReturn(GetMockedCompletionEventsUpdate(MaxEventsToFetch, MaxEventsToFetch));
			Org.Mockito.Mockito.When(umbilical.GetMapCompletionEvents(Matchers.Any<JobID>(), 
				Matchers.Eq(MaxEventsToFetch * 2), Matchers.Eq(MaxEventsToFetch), Matchers.Eq(tid
				))).ThenReturn(GetMockedCompletionEventsUpdate(MaxEventsToFetch * 2, 3));
			ShuffleScheduler<string, string> scheduler = Org.Mockito.Mockito.Mock<ShuffleScheduler
				>();
			ExceptionReporter reporter = Org.Mockito.Mockito.Mock<ExceptionReporter>();
			TestEventFetcher.EventFetcherForTest<string, string> ef = new TestEventFetcher.EventFetcherForTest
				<string, string>(tid, umbilical, scheduler, reporter, MaxEventsToFetch);
			ef.GetMapCompletionEvents();
			Org.Mockito.Mockito.Verify(reporter, Org.Mockito.Mockito.Never()).ReportException
				(Matchers.Any<Exception>());
			InOrder inOrder = Org.Mockito.Mockito.InOrder(umbilical);
			inOrder.Verify(umbilical).GetMapCompletionEvents(Matchers.Any<JobID>(), Matchers.Eq
				(0), Matchers.Eq(MaxEventsToFetch), Matchers.Eq(tid));
			inOrder.Verify(umbilical).GetMapCompletionEvents(Matchers.Any<JobID>(), Matchers.Eq
				(MaxEventsToFetch), Matchers.Eq(MaxEventsToFetch), Matchers.Eq(tid));
			inOrder.Verify(umbilical).GetMapCompletionEvents(Matchers.Any<JobID>(), Matchers.Eq
				(MaxEventsToFetch * 2), Matchers.Eq(MaxEventsToFetch), Matchers.Eq(tid));
			Org.Mockito.Mockito.Verify(scheduler, Org.Mockito.Mockito.Times(MaxEventsToFetch 
				* 2 + 3)).Resolve(Matchers.Any<TaskCompletionEvent>());
		}

		private MapTaskCompletionEventsUpdate GetMockedCompletionEventsUpdate(int startIdx
			, int numEvents)
		{
			AList<TaskCompletionEvent> tceList = new AList<TaskCompletionEvent>(numEvents);
			for (int i = 0; i < numEvents; ++i)
			{
				int eventIdx = startIdx + i;
				TaskCompletionEvent tce = new TaskCompletionEvent(eventIdx, new TaskAttemptID("12345"
					, 1, TaskType.Map, eventIdx, 0), eventIdx, true, TaskCompletionEvent.Status.Succeeded
					, "http://somehost:8888");
				tceList.AddItem(tce);
			}
			TaskCompletionEvent[] events = new TaskCompletionEvent[] {  };
			return new MapTaskCompletionEventsUpdate(Sharpen.Collections.ToArray(tceList, events
				), false);
		}

		private class EventFetcherForTest<K, V> : EventFetcher<K, V>
		{
			public EventFetcherForTest(TaskAttemptID reduce, TaskUmbilicalProtocol umbilical, 
				ShuffleScheduler<K, V> scheduler, ExceptionReporter reporter, int maxEventsToFetch
				)
				: base(reduce, umbilical, scheduler, reporter, maxEventsToFetch)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected internal override int GetMapCompletionEvents()
			{
				return base.GetMapCompletionEvents();
			}
		}
	}
}
