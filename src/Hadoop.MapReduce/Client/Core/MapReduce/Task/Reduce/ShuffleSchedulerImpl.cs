using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public class ShuffleSchedulerImpl<K, V> : ShuffleScheduler<K, V>
	{
		private sealed class _ThreadLocal_57 : ThreadLocal<long>
		{
			public _ThreadLocal_57()
			{
			}

			protected override long InitialValue()
			{
				return 0L;
			}
		}

		internal static ThreadLocal<long> shuffleStart = new _ThreadLocal_57();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.ShuffleSchedulerImpl
			));

		private const int MaxMapsAtOnce = 20;

		private const long InitialPenalty = 10000;

		private const float PenaltyGrowthRate = 1.3f;

		private const int ReportFailureLimit = 10;

		private const float BytesPerMillisToMbs = 1000f / 1024 / 1024;

		private readonly bool[] finishedMaps;

		private readonly int totalMaps;

		private int remainingMaps;

		private IDictionary<string, MapHost> mapLocations = new Dictionary<string, MapHost
			>();

		private ICollection<MapHost> pendingHosts = new HashSet<MapHost>();

		private ICollection<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();

		private readonly TaskAttemptID reduceId;

		private readonly Random random = new Random();

		private readonly DelayQueue<ShuffleSchedulerImpl.Penalty> penalties = new DelayQueue
			<ShuffleSchedulerImpl.Penalty>();

		private readonly ShuffleSchedulerImpl.Referee referee;

		private readonly IDictionary<TaskAttemptID, IntWritable> failureCounts = new Dictionary
			<TaskAttemptID, IntWritable>();

		private readonly IDictionary<string, IntWritable> hostFailures = new Dictionary<string
			, IntWritable>();

		private readonly TaskStatus status;

		private readonly ExceptionReporter reporter;

		private readonly int abortFailureLimit;

		private readonly Progress progress;

		private readonly Counters.Counter shuffledMapsCounter;

		private readonly Counters.Counter reduceShuffleBytes;

		private readonly Counters.Counter failedShuffleCounter;

		private readonly long startTime;

		private long lastProgressTime;

		private readonly ShuffleSchedulerImpl.CopyTimeTracker copyTimeTracker;

		private volatile int maxMapRuntime = 0;

		private readonly int maxFailedUniqueFetches;

		private readonly int maxFetchFailuresBeforeReporting;

		private long totalBytesShuffledTillNow = 0;

		private readonly DecimalFormat mbpsFormat = new DecimalFormat("0.00");

		private readonly bool reportReadErrorImmediately;

		private long maxDelay = MRJobConfig.DefaultMaxShuffleFetchRetryDelay;

		private int maxHostFailures;

		public ShuffleSchedulerImpl(JobConf job, TaskStatus status, TaskAttemptID reduceId
			, ExceptionReporter reporter, Progress progress, Counters.Counter shuffledMapsCounter
			, Counters.Counter reduceShuffleBytes, Counters.Counter failedShuffleCounter)
		{
			referee = new ShuffleSchedulerImpl.Referee(this);
			totalMaps = job.GetNumMapTasks();
			abortFailureLimit = Math.Max(30, totalMaps / 10);
			copyTimeTracker = new ShuffleSchedulerImpl.CopyTimeTracker();
			remainingMaps = totalMaps;
			finishedMaps = new bool[remainingMaps];
			this.reporter = reporter;
			this.status = status;
			this.reduceId = reduceId;
			this.progress = progress;
			this.shuffledMapsCounter = shuffledMapsCounter;
			this.reduceShuffleBytes = reduceShuffleBytes;
			this.failedShuffleCounter = failedShuffleCounter;
			this.startTime = Time.MonotonicNow();
			lastProgressTime = startTime;
			referee.Start();
			this.maxFailedUniqueFetches = Math.Min(totalMaps, 5);
			this.maxFetchFailuresBeforeReporting = job.GetInt(MRJobConfig.ShuffleFetchFailures
				, ReportFailureLimit);
			this.reportReadErrorImmediately = job.GetBoolean(MRJobConfig.ShuffleNotifyReaderror
				, true);
			this.maxDelay = job.GetLong(MRJobConfig.MaxShuffleFetchRetryDelay, MRJobConfig.DefaultMaxShuffleFetchRetryDelay
				);
			this.maxHostFailures = job.GetInt(MRJobConfig.MaxShuffleFetchHostFailures, MRJobConfig
				.DefaultMaxShuffleFetchHostFailures);
		}

		public virtual void Resolve(TaskCompletionEvent @event)
		{
			switch (@event.GetTaskStatus())
			{
				case TaskCompletionEvent.Status.Succeeded:
				{
					URI u = GetBaseURI(reduceId, @event.GetTaskTrackerHttp());
					AddKnownMapOutput(u.GetHost() + ":" + u.GetPort(), u.ToString(), ((TaskAttemptID)
						@event.GetTaskAttemptId()));
					maxMapRuntime = Math.Max(maxMapRuntime, @event.GetTaskRunTime());
					break;
				}

				case TaskCompletionEvent.Status.Failed:
				case TaskCompletionEvent.Status.Killed:
				case TaskCompletionEvent.Status.Obsolete:
				{
					ObsoleteMapOutput(((TaskAttemptID)@event.GetTaskAttemptId()));
					Log.Info("Ignoring obsolete output of " + @event.GetTaskStatus() + " map-task: '"
						 + ((TaskAttemptID)@event.GetTaskAttemptId()) + "'");
					break;
				}

				case TaskCompletionEvent.Status.Tipfailed:
				{
					TipFailed(((TaskID)((TaskAttemptID)@event.GetTaskAttemptId()).GetTaskID()));
					Log.Info("Ignoring output of failed map TIP: '" + ((TaskAttemptID)@event.GetTaskAttemptId
						()) + "'");
					break;
				}
			}
		}

		internal static URI GetBaseURI(TaskAttemptID reduceId, string url)
		{
			StringBuilder baseUrl = new StringBuilder(url);
			if (!url.EndsWith("/"))
			{
				baseUrl.Append("/");
			}
			baseUrl.Append("mapOutput?job=");
			baseUrl.Append(reduceId.GetJobID());
			baseUrl.Append("&reduce=");
			baseUrl.Append(reduceId.GetTaskID().GetId());
			baseUrl.Append("&map=");
			URI u = URI.Create(baseUrl.ToString());
			return u;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CopySucceeded(TaskAttemptID mapId, MapHost host, long bytes, 
			long startMillis, long endMillis, MapOutput<K, V> output)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(failureCounts, mapId);
				Sharpen.Collections.Remove(hostFailures, host.GetHostName());
				int mapIndex = mapId.GetTaskID().GetId();
				if (!finishedMaps[mapIndex])
				{
					output.Commit();
					finishedMaps[mapIndex] = true;
					shuffledMapsCounter.Increment(1);
					if (--remainingMaps == 0)
					{
						Sharpen.Runtime.NotifyAll(this);
					}
					// update single copy task status
					long copyMillis = (endMillis - startMillis);
					if (copyMillis == 0)
					{
						copyMillis = 1;
					}
					float bytesPerMillis = (float)bytes / copyMillis;
					float transferRate = bytesPerMillis * BytesPerMillisToMbs;
					string individualProgress = "copy task(" + mapId + " succeeded" + " at " + mbpsFormat
						.Format(transferRate) + " MB/s)";
					// update the aggregated status
					copyTimeTracker.Add(startMillis, endMillis);
					totalBytesShuffledTillNow += bytes;
					UpdateStatus(individualProgress);
					reduceShuffleBytes.Increment(bytes);
					lastProgressTime = Time.MonotonicNow();
					Log.Debug("map " + mapId + " done " + status.GetStateString());
				}
			}
		}

		private void UpdateStatus(string individualProgress)
		{
			lock (this)
			{
				int mapsDone = totalMaps - remainingMaps;
				long totalCopyMillis = copyTimeTracker.GetCopyMillis();
				if (totalCopyMillis == 0)
				{
					totalCopyMillis = 1;
				}
				float bytesPerMillis = (float)totalBytesShuffledTillNow / totalCopyMillis;
				float transferRate = bytesPerMillis * BytesPerMillisToMbs;
				progress.Set((float)mapsDone / totalMaps);
				string statusString = mapsDone + " / " + totalMaps + " copied.";
				status.SetStateString(statusString);
				if (individualProgress != null)
				{
					progress.SetStatus(individualProgress + " Aggregated copy rate(" + mapsDone + " of "
						 + totalMaps + " at " + mbpsFormat.Format(transferRate) + " MB/s)");
				}
				else
				{
					progress.SetStatus("copy(" + mapsDone + " of " + totalMaps + " at " + mbpsFormat.
						Format(transferRate) + " MB/s)");
				}
			}
		}

		private void UpdateStatus()
		{
			UpdateStatus(null);
		}

		public virtual void HostFailed(string hostname)
		{
			lock (this)
			{
				if (hostFailures.Contains(hostname))
				{
					IntWritable x = hostFailures[hostname];
					x.Set(x.Get() + 1);
				}
				else
				{
					hostFailures[hostname] = new IntWritable(1);
				}
			}
		}

		public virtual void CopyFailed(TaskAttemptID mapId, MapHost host, bool readError, 
			bool connectExcpt)
		{
			lock (this)
			{
				host.Penalize();
				int failures = 1;
				if (failureCounts.Contains(mapId))
				{
					IntWritable x = failureCounts[mapId];
					x.Set(x.Get() + 1);
					failures = x.Get();
				}
				else
				{
					failureCounts[mapId] = new IntWritable(1);
				}
				string hostname = host.GetHostName();
				IntWritable hostFailedNum = hostFailures[hostname];
				// MAPREDUCE-6361: hostname could get cleanup from hostFailures in another
				// thread with copySucceeded.
				// In this case, add back hostname to hostFailures to get rid of NPE issue.
				if (hostFailedNum == null)
				{
					hostFailures[hostname] = new IntWritable(1);
				}
				//report failure if already retried maxHostFailures times
				bool hostFail = hostFailures[hostname].Get() > GetMaxHostFailures() ? true : false;
				if (failures >= abortFailureLimit)
				{
					try
					{
						throw new IOException(failures + " failures downloading " + mapId);
					}
					catch (IOException ie)
					{
						reporter.ReportException(ie);
					}
				}
				CheckAndInformMRAppMaster(failures, mapId, readError, connectExcpt, hostFail);
				CheckReducerHealth();
				long delay = (long)(InitialPenalty * Math.Pow(PenaltyGrowthRate, failures));
				if (delay > maxDelay)
				{
					delay = maxDelay;
				}
				penalties.AddItem(new ShuffleSchedulerImpl.Penalty(host, delay));
				failedShuffleCounter.Increment(1);
			}
		}

		public virtual void ReportLocalError(IOException ioe)
		{
			try
			{
				Log.Error("Shuffle failed : local error on this node: " + Sharpen.Runtime.GetLocalHost
					());
			}
			catch (UnknownHostException)
			{
				Log.Error("Shuffle failed : local error on this node");
			}
			reporter.ReportException(ioe);
		}

		// Notify the MRAppMaster
		// after every read error, if 'reportReadErrorImmediately' is true or
		// after every 'maxFetchFailuresBeforeReporting' failures
		private void CheckAndInformMRAppMaster(int failures, TaskAttemptID mapId, bool readError
			, bool connectExcpt, bool hostFailed)
		{
			if (connectExcpt || (reportReadErrorImmediately && readError) || ((failures % maxFetchFailuresBeforeReporting
				) == 0) || hostFailed)
			{
				Log.Info("Reporting fetch failure for " + mapId + " to MRAppMaster.");
				status.AddFetchFailedMap((TaskAttemptID)mapId);
			}
		}

		private void CheckReducerHealth()
		{
			float MaxAllowedFailedFetchAttemptPercent = 0.5f;
			float MinRequiredProgressPercent = 0.5f;
			float MaxAllowedStallTimePercent = 0.5f;
			long totalFailures = failedShuffleCounter.GetValue();
			int doneMaps = totalMaps - remainingMaps;
			bool reducerHealthy = (((float)totalFailures / (totalFailures + doneMaps)) < MaxAllowedFailedFetchAttemptPercent
				);
			// check if the reducer has progressed enough
			bool reducerProgressedEnough = (((float)doneMaps / totalMaps) >= MinRequiredProgressPercent
				);
			// check if the reducer is stalled for a long time
			// duration for which the reducer is stalled
			int stallDuration = (int)(Time.MonotonicNow() - lastProgressTime);
			// duration for which the reducer ran with progress
			int shuffleProgressDuration = (int)(lastProgressTime - startTime);
			// min time the reducer should run without getting killed
			int minShuffleRunDuration = Math.Max(shuffleProgressDuration, maxMapRuntime);
			bool reducerStalled = (((float)stallDuration / minShuffleRunDuration) >= MaxAllowedStallTimePercent
				);
			// kill if not healthy and has insufficient progress
			if ((failureCounts.Count >= maxFailedUniqueFetches || failureCounts.Count == (totalMaps
				 - doneMaps)) && !reducerHealthy && (!reducerProgressedEnough || reducerStalled))
			{
				Log.Fatal("Shuffle failed with too many fetch failures " + "and insufficient progress!"
					);
				string errorMsg = "Exceeded MAX_FAILED_UNIQUE_FETCHES; bailing-out.";
				reporter.ReportException(new IOException(errorMsg));
			}
		}

		public virtual void TipFailed(TaskID taskId)
		{
			lock (this)
			{
				if (!finishedMaps[taskId.GetId()])
				{
					finishedMaps[taskId.GetId()] = true;
					if (--remainingMaps == 0)
					{
						Sharpen.Runtime.NotifyAll(this);
					}
					UpdateStatus();
				}
			}
		}

		public virtual void AddKnownMapOutput(string hostName, string hostUrl, TaskAttemptID
			 mapId)
		{
			lock (this)
			{
				MapHost host = mapLocations[hostName];
				if (host == null)
				{
					host = new MapHost(hostName, hostUrl);
					mapLocations[hostName] = host;
				}
				host.AddKnownMap(mapId);
				// Mark the host as pending
				if (host.GetState() == MapHost.State.Pending)
				{
					pendingHosts.AddItem(host);
					Sharpen.Runtime.NotifyAll(this);
				}
			}
		}

		public virtual void ObsoleteMapOutput(TaskAttemptID mapId)
		{
			lock (this)
			{
				obsoleteMaps.AddItem(mapId);
			}
		}

		public virtual void PutBackKnownMapOutput(MapHost host, TaskAttemptID mapId)
		{
			lock (this)
			{
				host.AddKnownMap(mapId);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual MapHost GetHost()
		{
			lock (this)
			{
				while (pendingHosts.IsEmpty())
				{
					Sharpen.Runtime.Wait(this);
				}
				MapHost host = null;
				IEnumerator<MapHost> iter = pendingHosts.GetEnumerator();
				int numToPick = random.Next(pendingHosts.Count);
				for (int i = 0; i <= numToPick; ++i)
				{
					host = iter.Next();
				}
				pendingHosts.Remove(host);
				host.MarkBusy();
				Log.Info("Assigning " + host + " with " + host.GetNumKnownMapOutputs() + " to " +
					 Sharpen.Thread.CurrentThread().GetName());
				shuffleStart.Set(Time.MonotonicNow());
				return host;
			}
		}

		public virtual IList<TaskAttemptID> GetMapsForHost(MapHost host)
		{
			lock (this)
			{
				IList<TaskAttemptID> list = host.GetAndClearKnownMaps();
				IEnumerator<TaskAttemptID> itr = list.GetEnumerator();
				IList<TaskAttemptID> result = new AList<TaskAttemptID>();
				int includedMaps = 0;
				int totalSize = list.Count;
				// find the maps that we still need, up to the limit
				while (itr.HasNext())
				{
					TaskAttemptID id = itr.Next();
					if (!obsoleteMaps.Contains(id) && !finishedMaps[id.GetTaskID().GetId()])
					{
						result.AddItem(id);
						if (++includedMaps >= MaxMapsAtOnce)
						{
							break;
						}
					}
				}
				// put back the maps left after the limit
				while (itr.HasNext())
				{
					TaskAttemptID id = itr.Next();
					if (!obsoleteMaps.Contains(id) && !finishedMaps[id.GetTaskID().GetId()])
					{
						host.AddKnownMap(id);
					}
				}
				Log.Info("assigned " + includedMaps + " of " + totalSize + " to " + host + " to "
					 + Sharpen.Thread.CurrentThread().GetName());
				return result;
			}
		}

		public virtual void FreeHost(MapHost host)
		{
			lock (this)
			{
				if (host.GetState() != MapHost.State.Penalized)
				{
					if (host.MarkAvailable() == MapHost.State.Pending)
					{
						pendingHosts.AddItem(host);
						Sharpen.Runtime.NotifyAll(this);
					}
				}
				Log.Info(host + " freed by " + Sharpen.Thread.CurrentThread().GetName() + " in " 
					+ (Time.MonotonicNow() - shuffleStart.Get()) + "ms");
			}
		}

		public virtual void ResetKnownMaps()
		{
			lock (this)
			{
				mapLocations.Clear();
				obsoleteMaps.Clear();
				pendingHosts.Clear();
			}
		}

		/// <summary>Wait until the shuffle finishes or until the timeout.</summary>
		/// <param name="millis">maximum wait time</param>
		/// <returns>true if the shuffle is done</returns>
		/// <exception cref="System.Exception"/>
		public virtual bool WaitUntilDone(int millis)
		{
			lock (this)
			{
				if (remainingMaps > 0)
				{
					Sharpen.Runtime.Wait(this, millis);
					return remainingMaps == 0;
				}
				return true;
			}
		}

		/// <summary>A structure that records the penalty for a host.</summary>
		private class Penalty : Delayed
		{
			internal MapHost host;

			private long endTime;

			internal Penalty(MapHost host, long delay)
			{
				this.host = host;
				this.endTime = Time.MonotonicNow() + delay;
			}

			public virtual long GetDelay(TimeUnit unit)
			{
				long remainingTime = endTime - Time.MonotonicNow();
				return unit.Convert(remainingTime, TimeUnit.Milliseconds);
			}

			public virtual int CompareTo(Delayed o)
			{
				long other = ((ShuffleSchedulerImpl.Penalty)o).endTime;
				return endTime == other ? 0 : (endTime < other ? -1 : 1);
			}
		}

		/// <summary>A thread that takes hosts off of the penalty list when the timer expires.
		/// 	</summary>
		private class Referee : Sharpen.Thread
		{
			public Referee(ShuffleSchedulerImpl<K, V> _enclosing)
			{
				this._enclosing = _enclosing;
				this.SetName("ShufflePenaltyReferee");
				this.SetDaemon(true);
			}

			public override void Run()
			{
				try
				{
					while (true)
					{
						// take the first host that has an expired penalty
						MapHost host = this._enclosing.penalties.Take().host;
						lock (this._enclosing._enclosing)
						{
							if (host.MarkAvailable() == MapHost.State.Pending)
							{
								this._enclosing.pendingHosts.AddItem(host);
								Sharpen.Runtime.NotifyAll(this._enclosing._enclosing);
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
					this._enclosing.reporter.ReportException(t);
				}
			}

			private readonly ShuffleSchedulerImpl<K, V> _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void Close()
		{
			referee.Interrupt();
			referee.Join();
		}

		public virtual int GetMaxHostFailures()
		{
			return maxHostFailures;
		}

		private class CopyTimeTracker
		{
			internal IList<ShuffleSchedulerImpl.CopyTimeTracker.Interval> intervals;

			internal long copyMillis;

			public CopyTimeTracker()
			{
				intervals = Sharpen.Collections.EmptyList();
				copyMillis = 0;
			}

			public virtual void Add(long s, long e)
			{
				ShuffleSchedulerImpl.CopyTimeTracker.Interval interval = new ShuffleSchedulerImpl.CopyTimeTracker.Interval
					(s, e);
				copyMillis = GetTotalCopyMillis(interval);
			}

			public virtual long GetCopyMillis()
			{
				return copyMillis;
			}

			// This method captures the time during which any copy was in progress 
			// each copy time period is record in the Interval list
			private long GetTotalCopyMillis(ShuffleSchedulerImpl.CopyTimeTracker.Interval newInterval
				)
			{
				if (newInterval == null)
				{
					return copyMillis;
				}
				IList<ShuffleSchedulerImpl.CopyTimeTracker.Interval> result = new AList<ShuffleSchedulerImpl.CopyTimeTracker.Interval
					>(intervals.Count + 1);
				foreach (ShuffleSchedulerImpl.CopyTimeTracker.Interval interval in intervals)
				{
					if (interval.end < newInterval.start)
					{
						result.AddItem(interval);
					}
					else
					{
						if (interval.start > newInterval.end)
						{
							result.AddItem(newInterval);
							newInterval = interval;
						}
						else
						{
							newInterval = new ShuffleSchedulerImpl.CopyTimeTracker.Interval(Math.Min(interval
								.start, newInterval.start), Math.Max(newInterval.end, interval.end));
						}
					}
				}
				result.AddItem(newInterval);
				intervals = result;
				//compute total millis
				long length = 0;
				foreach (ShuffleSchedulerImpl.CopyTimeTracker.Interval interval_1 in intervals)
				{
					length += interval_1.GetIntervalLength();
				}
				return length;
			}

			private class Interval
			{
				internal readonly long start;

				internal readonly long end;

				public Interval(long s, long e)
				{
					start = s;
					end = e;
				}

				public virtual long GetIntervalLength()
				{
					return end - start;
				}
			}
		}
	}
}
