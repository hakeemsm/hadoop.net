using System;
using System.Threading;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>An adapter class for metrics sink and associated filters</summary>
	internal class MetricsSinkAdapter : SinkQueue.Consumer<MetricsBuffer>
	{
		private readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics2.Impl.MetricsSinkAdapter
			));

		private readonly string name;

		private readonly string description;

		private readonly string context;

		private readonly MetricsSink sink;

		private readonly MetricsFilter sourceFilter;

		private readonly MetricsFilter recordFilter;

		private readonly MetricsFilter metricFilter;

		private readonly SinkQueue<MetricsBuffer> queue;

		private readonly Thread sinkThread;

		private volatile bool stopping = false;

		private volatile bool inError = false;

		private readonly int period;

		private readonly int firstRetryDelay;

		private readonly int retryCount;

		private readonly long oobPutTimeout;

		private readonly float retryBackoff;

		private readonly MetricsRegistry registry = new MetricsRegistry("sinkadapter");

		private readonly MutableStat latency;

		private readonly MutableCounterInt dropped;

		private readonly MutableGaugeInt qsize;

		internal MetricsSinkAdapter(string name, string description, MetricsSink sink, string
			 context, MetricsFilter sourceFilter, MetricsFilter recordFilter, MetricsFilter 
			metricFilter, int period, int queueCapacity, int retryDelay, float retryBackoff, 
			int retryCount)
		{
			this.name = Preconditions.CheckNotNull(name, "name");
			this.description = description;
			this.sink = Preconditions.CheckNotNull(sink, "sink object");
			this.context = context;
			this.sourceFilter = sourceFilter;
			this.recordFilter = recordFilter;
			this.metricFilter = metricFilter;
			this.period = Contracts.CheckArg(period, period > 0, "period");
			firstRetryDelay = Contracts.CheckArg(retryDelay, retryDelay > 0, "retry delay");
			this.retryBackoff = Contracts.CheckArg(retryBackoff, retryBackoff > 1, "retry backoff"
				);
			oobPutTimeout = (long)(firstRetryDelay * Math.Pow(retryBackoff, retryCount) * 1000
				);
			this.retryCount = retryCount;
			this.queue = new SinkQueue<MetricsBuffer>(Contracts.CheckArg(queueCapacity, queueCapacity
				 > 0, "queue capacity"));
			latency = registry.NewRate("Sink_" + name, "Sink end to end latency", false);
			dropped = registry.NewCounter("Sink_" + name + "Dropped", "Dropped updates per sink"
				, 0);
			qsize = registry.NewGauge("Sink_" + name + "Qsize", "Queue size", 0);
			sinkThread = new _Thread_86(this);
			sinkThread.SetName(name);
			sinkThread.SetDaemon(true);
		}

		private sealed class _Thread_86 : Thread
		{
			public _Thread_86(MetricsSinkAdapter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				this._enclosing.PublishMetricsFromQueue();
			}

			private readonly MetricsSinkAdapter _enclosing;
		}

		internal virtual bool PutMetrics(MetricsBuffer buffer, long logicalTime)
		{
			if (logicalTime % period == 0)
			{
				Log.Debug("enqueue, logicalTime=" + logicalTime);
				if (queue.Enqueue(buffer))
				{
					RefreshQueueSizeGauge();
					return true;
				}
				dropped.Incr();
				return false;
			}
			return true;
		}

		// OK
		public virtual bool PutMetricsImmediate(MetricsBuffer buffer)
		{
			MetricsSinkAdapter.WaitableMetricsBuffer waitableBuffer = new MetricsSinkAdapter.WaitableMetricsBuffer
				(buffer);
			if (queue.Enqueue(waitableBuffer))
			{
				RefreshQueueSizeGauge();
			}
			else
			{
				Log.Warn(name + " has a full queue and can't consume the given metrics.");
				dropped.Incr();
				return false;
			}
			if (!waitableBuffer.WaitTillNotified(oobPutTimeout))
			{
				Log.Warn(name + " couldn't fulfill an immediate putMetrics request in time." + " Abandoning."
					);
				return false;
			}
			return true;
		}

		internal virtual void PublishMetricsFromQueue()
		{
			int retryDelay = firstRetryDelay;
			int n = retryCount;
			int minDelay = Math.Min(500, retryDelay * 1000);
			// millis
			Random rng = new Random(Runtime.NanoTime());
			while (!stopping)
			{
				try
				{
					queue.ConsumeAll(this);
					RefreshQueueSizeGauge();
					retryDelay = firstRetryDelay;
					n = retryCount;
					inError = false;
				}
				catch (Exception)
				{
					Log.Info(name + " thread interrupted.");
				}
				catch (Exception e)
				{
					if (n > 0)
					{
						int retryWindow = Math.Max(0, 1000 / 2 * retryDelay - minDelay);
						int awhile = rng.Next(retryWindow) + minDelay;
						if (!inError)
						{
							Log.Error("Got sink exception, retry in " + awhile + "ms", e);
						}
						retryDelay *= retryBackoff;
						try
						{
							Thread.Sleep(awhile);
						}
						catch (Exception e2)
						{
							Log.Info(name + " thread interrupted while waiting for retry", e2);
						}
						--n;
					}
					else
					{
						if (!inError)
						{
							Log.Error("Got sink exception and over retry limit, " + "suppressing further error messages"
								, e);
						}
						queue.Clear();
						RefreshQueueSizeGauge();
						inError = true;
					}
				}
			}
		}

		// Don't keep complaining ad infinitum
		private void RefreshQueueSizeGauge()
		{
			qsize.Set(queue.Size());
		}

		public virtual void Consume(MetricsBuffer buffer)
		{
			long ts = 0;
			foreach (MetricsBuffer.Entry entry in buffer)
			{
				if (sourceFilter == null || sourceFilter.Accepts(entry.Name()))
				{
					foreach (MetricsRecordImpl record in entry.Records())
					{
						if ((context == null || context.Equals(record.Context())) && (recordFilter == null
							 || recordFilter.Accepts(record)))
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Pushing record " + entry.Name() + "." + record.Context() + "." + record
									.Name() + " to " + name);
							}
							sink.PutMetrics(metricFilter == null ? record : new MetricsRecordFiltered(record, 
								metricFilter));
							if (ts == 0)
							{
								ts = record.Timestamp();
							}
						}
					}
				}
			}
			if (ts > 0)
			{
				sink.Flush();
				latency.Add(Time.Now() - ts);
			}
			if (buffer is MetricsSinkAdapter.WaitableMetricsBuffer)
			{
				((MetricsSinkAdapter.WaitableMetricsBuffer)buffer).NotifyAnyWaiters();
			}
			Log.Debug("Done");
		}

		internal virtual void Start()
		{
			sinkThread.Start();
			Log.Info("Sink " + name + " started");
		}

		internal virtual void Stop()
		{
			stopping = true;
			sinkThread.Interrupt();
			if (sink is IDisposable)
			{
				IOUtils.Cleanup(Log, (IDisposable)sink);
			}
			try
			{
				sinkThread.Join();
			}
			catch (Exception e)
			{
				Log.Warn("Stop interrupted", e);
			}
		}

		internal virtual string Name()
		{
			return name;
		}

		internal virtual string Description()
		{
			return description;
		}

		internal virtual void Snapshot(MetricsRecordBuilder rb, bool all)
		{
			registry.Snapshot(rb, all);
		}

		internal virtual MetricsSink Sink()
		{
			return sink;
		}

		internal class WaitableMetricsBuffer : MetricsBuffer
		{
			private readonly Semaphore notificationSemaphore = Extensions.CreateSemaphore
				(0);

			public WaitableMetricsBuffer(MetricsBuffer metricsBuffer)
				: base(metricsBuffer)
			{
			}

			public virtual bool WaitTillNotified(long millisecondsToWait)
			{
				try
				{
					return notificationSemaphore.TryAcquire(millisecondsToWait, TimeUnit.Milliseconds
						);
				}
				catch (Exception)
				{
					return false;
				}
			}

			public virtual void NotifyAnyWaiters()
			{
				notificationSemaphore.Release();
			}
		}
	}
}
