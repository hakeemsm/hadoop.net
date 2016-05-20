using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>An adapter class for metrics sink and associated filters</summary>
	internal class MetricsSinkAdapter : org.apache.hadoop.metrics2.impl.SinkQueue.Consumer
		<org.apache.hadoop.metrics2.impl.MetricsBuffer>
	{
		private readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics2.impl.MetricsSinkAdapter
			)));

		private readonly string name;

		private readonly string description;

		private readonly string context;

		private readonly org.apache.hadoop.metrics2.MetricsSink sink;

		private readonly org.apache.hadoop.metrics2.MetricsFilter sourceFilter;

		private readonly org.apache.hadoop.metrics2.MetricsFilter recordFilter;

		private readonly org.apache.hadoop.metrics2.MetricsFilter metricFilter;

		private readonly org.apache.hadoop.metrics2.impl.SinkQueue<org.apache.hadoop.metrics2.impl.MetricsBuffer
			> queue;

		private readonly java.lang.Thread sinkThread;

		private volatile bool stopping = false;

		private volatile bool inError = false;

		private readonly int period;

		private readonly int firstRetryDelay;

		private readonly int retryCount;

		private readonly long oobPutTimeout;

		private readonly float retryBackoff;

		private readonly org.apache.hadoop.metrics2.lib.MetricsRegistry registry = new org.apache.hadoop.metrics2.lib.MetricsRegistry
			("sinkadapter");

		private readonly org.apache.hadoop.metrics2.lib.MutableStat latency;

		private readonly org.apache.hadoop.metrics2.lib.MutableCounterInt dropped;

		private readonly org.apache.hadoop.metrics2.lib.MutableGaugeInt qsize;

		internal MetricsSinkAdapter(string name, string description, org.apache.hadoop.metrics2.MetricsSink
			 sink, string context, org.apache.hadoop.metrics2.MetricsFilter sourceFilter, org.apache.hadoop.metrics2.MetricsFilter
			 recordFilter, org.apache.hadoop.metrics2.MetricsFilter metricFilter, int period
			, int queueCapacity, int retryDelay, float retryBackoff, int retryCount)
		{
			this.name = com.google.common.@base.Preconditions.checkNotNull(name, "name");
			this.description = description;
			this.sink = com.google.common.@base.Preconditions.checkNotNull(sink, "sink object"
				);
			this.context = context;
			this.sourceFilter = sourceFilter;
			this.recordFilter = recordFilter;
			this.metricFilter = metricFilter;
			this.period = org.apache.hadoop.metrics2.util.Contracts.checkArg(period, period >
				 0, "period");
			firstRetryDelay = org.apache.hadoop.metrics2.util.Contracts.checkArg(retryDelay, 
				retryDelay > 0, "retry delay");
			this.retryBackoff = org.apache.hadoop.metrics2.util.Contracts.checkArg(retryBackoff
				, retryBackoff > 1, "retry backoff");
			oobPutTimeout = (long)(firstRetryDelay * System.Math.pow(retryBackoff, retryCount
				) * 1000);
			this.retryCount = retryCount;
			this.queue = new org.apache.hadoop.metrics2.impl.SinkQueue<org.apache.hadoop.metrics2.impl.MetricsBuffer
				>(org.apache.hadoop.metrics2.util.Contracts.checkArg(queueCapacity, queueCapacity
				 > 0, "queue capacity"));
			latency = registry.newRate("Sink_" + name, "Sink end to end latency", false);
			dropped = registry.newCounter("Sink_" + name + "Dropped", "Dropped updates per sink"
				, 0);
			qsize = registry.newGauge("Sink_" + name + "Qsize", "Queue size", 0);
			sinkThread = new _Thread_86(this);
			sinkThread.setName(name);
			sinkThread.setDaemon(true);
		}

		private sealed class _Thread_86 : java.lang.Thread
		{
			public _Thread_86(MetricsSinkAdapter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void run()
			{
				this._enclosing.publishMetricsFromQueue();
			}

			private readonly MetricsSinkAdapter _enclosing;
		}

		internal virtual bool putMetrics(org.apache.hadoop.metrics2.impl.MetricsBuffer buffer
			, long logicalTime)
		{
			if (logicalTime % period == 0)
			{
				LOG.debug("enqueue, logicalTime=" + logicalTime);
				if (queue.enqueue(buffer))
				{
					refreshQueueSizeGauge();
					return true;
				}
				dropped.incr();
				return false;
			}
			return true;
		}

		// OK
		public virtual bool putMetricsImmediate(org.apache.hadoop.metrics2.impl.MetricsBuffer
			 buffer)
		{
			org.apache.hadoop.metrics2.impl.MetricsSinkAdapter.WaitableMetricsBuffer waitableBuffer
				 = new org.apache.hadoop.metrics2.impl.MetricsSinkAdapter.WaitableMetricsBuffer(
				buffer);
			if (queue.enqueue(waitableBuffer))
			{
				refreshQueueSizeGauge();
			}
			else
			{
				LOG.warn(name + " has a full queue and can't consume the given metrics.");
				dropped.incr();
				return false;
			}
			if (!waitableBuffer.waitTillNotified(oobPutTimeout))
			{
				LOG.warn(name + " couldn't fulfill an immediate putMetrics request in time." + " Abandoning."
					);
				return false;
			}
			return true;
		}

		internal virtual void publishMetricsFromQueue()
		{
			int retryDelay = firstRetryDelay;
			int n = retryCount;
			int minDelay = System.Math.min(500, retryDelay * 1000);
			// millis
			java.util.Random rng = new java.util.Random(Sharpen.Runtime.nanoTime());
			while (!stopping)
			{
				try
				{
					queue.consumeAll(this);
					refreshQueueSizeGauge();
					retryDelay = firstRetryDelay;
					n = retryCount;
					inError = false;
				}
				catch (System.Exception)
				{
					LOG.info(name + " thread interrupted.");
				}
				catch (System.Exception e)
				{
					if (n > 0)
					{
						int retryWindow = System.Math.max(0, 1000 / 2 * retryDelay - minDelay);
						int awhile = rng.nextInt(retryWindow) + minDelay;
						if (!inError)
						{
							LOG.error("Got sink exception, retry in " + awhile + "ms", e);
						}
						retryDelay *= retryBackoff;
						try
						{
							java.lang.Thread.sleep(awhile);
						}
						catch (System.Exception e2)
						{
							LOG.info(name + " thread interrupted while waiting for retry", e2);
						}
						--n;
					}
					else
					{
						if (!inError)
						{
							LOG.error("Got sink exception and over retry limit, " + "suppressing further error messages"
								, e);
						}
						queue.clear();
						refreshQueueSizeGauge();
						inError = true;
					}
				}
			}
		}

		// Don't keep complaining ad infinitum
		private void refreshQueueSizeGauge()
		{
			qsize.set(queue.size());
		}

		public virtual void consume(org.apache.hadoop.metrics2.impl.MetricsBuffer buffer)
		{
			long ts = 0;
			foreach (org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry entry in buffer)
			{
				if (sourceFilter == null || sourceFilter.accepts(entry.name()))
				{
					foreach (org.apache.hadoop.metrics2.impl.MetricsRecordImpl record in entry.records
						())
					{
						if ((context == null || context.Equals(record.context())) && (recordFilter == null
							 || recordFilter.accepts(record)))
						{
							if (LOG.isDebugEnabled())
							{
								LOG.debug("Pushing record " + entry.name() + "." + record.context() + "." + record
									.name() + " to " + name);
							}
							sink.putMetrics(metricFilter == null ? record : new org.apache.hadoop.metrics2.impl.MetricsRecordFiltered
								(record, metricFilter));
							if (ts == 0)
							{
								ts = record.timestamp();
							}
						}
					}
				}
			}
			if (ts > 0)
			{
				sink.flush();
				latency.add(org.apache.hadoop.util.Time.now() - ts);
			}
			if (buffer is org.apache.hadoop.metrics2.impl.MetricsSinkAdapter.WaitableMetricsBuffer)
			{
				((org.apache.hadoop.metrics2.impl.MetricsSinkAdapter.WaitableMetricsBuffer)buffer
					).notifyAnyWaiters();
			}
			LOG.debug("Done");
		}

		internal virtual void start()
		{
			sinkThread.start();
			LOG.info("Sink " + name + " started");
		}

		internal virtual void stop()
		{
			stopping = true;
			sinkThread.interrupt();
			if (sink is java.io.Closeable)
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, (java.io.Closeable)sink);
			}
			try
			{
				sinkThread.join();
			}
			catch (System.Exception e)
			{
				LOG.warn("Stop interrupted", e);
			}
		}

		internal virtual string name()
		{
			return name;
		}

		internal virtual string description()
		{
			return description;
		}

		internal virtual void snapshot(org.apache.hadoop.metrics2.MetricsRecordBuilder rb
			, bool all)
		{
			registry.snapshot(rb, all);
		}

		internal virtual org.apache.hadoop.metrics2.MetricsSink sink()
		{
			return sink;
		}

		internal class WaitableMetricsBuffer : org.apache.hadoop.metrics2.impl.MetricsBuffer
		{
			private readonly java.util.concurrent.Semaphore notificationSemaphore = new java.util.concurrent.Semaphore
				(0);

			public WaitableMetricsBuffer(org.apache.hadoop.metrics2.impl.MetricsBuffer metricsBuffer
				)
				: base(metricsBuffer)
			{
			}

			public virtual bool waitTillNotified(long millisecondsToWait)
			{
				try
				{
					return notificationSemaphore.tryAcquire(millisecondsToWait, java.util.concurrent.TimeUnit
						.MILLISECONDS);
				}
				catch (System.Exception)
				{
					return false;
				}
			}

			public virtual void notifyAnyWaiters()
			{
				notificationSemaphore.release();
			}
		}
	}
}
