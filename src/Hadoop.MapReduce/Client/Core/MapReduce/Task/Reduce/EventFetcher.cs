using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	internal class EventFetcher<K, V> : Sharpen.Thread
	{
		private const long SleepTime = 1000;

		private const int MaxRetries = 10;

		private const int RetryPeriod = 5000;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.EventFetcher
			));

		private readonly TaskAttemptID reduce;

		private readonly TaskUmbilicalProtocol umbilical;

		private readonly ShuffleScheduler<K, V> scheduler;

		private int fromEventIdx = 0;

		private readonly int maxEventsToFetch;

		private readonly ExceptionReporter exceptionReporter;

		private volatile bool stopped = false;

		public EventFetcher(TaskAttemptID reduce, TaskUmbilicalProtocol umbilical, ShuffleScheduler
			<K, V> scheduler, ExceptionReporter reporter, int maxEventsToFetch)
		{
			SetName("EventFetcher for fetching Map Completion Events");
			SetDaemon(true);
			this.reduce = reduce;
			this.umbilical = umbilical;
			this.scheduler = scheduler;
			exceptionReporter = reporter;
			this.maxEventsToFetch = maxEventsToFetch;
		}

		public override void Run()
		{
			int failures = 0;
			Log.Info(reduce + " Thread started: " + GetName());
			try
			{
				while (!stopped && !Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					try
					{
						int numNewMaps = GetMapCompletionEvents();
						failures = 0;
						if (numNewMaps > 0)
						{
							Log.Info(reduce + ": " + "Got " + numNewMaps + " new map-outputs");
						}
						Log.Debug("GetMapEventsThread about to sleep for " + SleepTime);
						if (!Sharpen.Thread.CurrentThread().IsInterrupted())
						{
							Sharpen.Thread.Sleep(SleepTime);
						}
					}
					catch (Exception)
					{
						Log.Info("EventFetcher is interrupted.. Returning");
						return;
					}
					catch (IOException ie)
					{
						Log.Info("Exception in getting events", ie);
						// check to see whether to abort
						if (++failures >= MaxRetries)
						{
							throw new IOException("too many failures downloading events", ie);
						}
						// sleep for a bit
						if (!Sharpen.Thread.CurrentThread().IsInterrupted())
						{
							Sharpen.Thread.Sleep(RetryPeriod);
						}
					}
				}
			}
			catch (Exception)
			{
				return;
			}
			catch (Exception t)
			{
				exceptionReporter.ReportException(t);
				return;
			}
		}

		public virtual void ShutDown()
		{
			this.stopped = true;
			Interrupt();
			try
			{
				Join(5000);
			}
			catch (Exception ie)
			{
				Log.Warn("Got interrupted while joining " + GetName(), ie);
			}
		}

		/// <summary>
		/// Queries the
		/// <see cref="TaskTracker"/>
		/// for a set of map-completion events
		/// from a given event ID.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual int GetMapCompletionEvents()
		{
			int numNewMaps = 0;
			TaskCompletionEvent[] events = null;
			do
			{
				MapTaskCompletionEventsUpdate update = umbilical.GetMapCompletionEvents((JobID)reduce
					.GetJobID(), fromEventIdx, maxEventsToFetch, (TaskAttemptID)reduce);
				events = update.GetMapTaskCompletionEvents();
				Log.Debug("Got " + events.Length + " map completion events from " + fromEventIdx);
				System.Diagnostics.Debug.Assert(!update.ShouldReset(), "Unexpected legacy state");
				// Update the last seen event ID
				fromEventIdx += events.Length;
				// Process the TaskCompletionEvents:
				// 1. Save the SUCCEEDED maps in knownOutputs to fetch the outputs.
				// 2. Save the OBSOLETE/FAILED/KILLED maps in obsoleteOutputs to stop
				//    fetching from those maps.
				// 3. Remove TIPFAILED maps from neededOutputs since we don't need their
				//    outputs at all.
				foreach (TaskCompletionEvent @event in events)
				{
					scheduler.Resolve(@event);
					if (TaskCompletionEvent.Status.Succeeded == @event.GetTaskStatus())
					{
						++numNewMaps;
					}
				}
			}
			while (events.Length == maxEventsToFetch);
			return numNewMaps;
		}
	}
}
