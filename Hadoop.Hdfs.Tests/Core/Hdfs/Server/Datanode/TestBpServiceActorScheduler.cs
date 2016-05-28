using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Commons.Logging;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Verify the block report and heartbeat scheduling logic of BPServiceActor
	/// using a few different values .
	/// </summary>
	public class TestBpServiceActorScheduler
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestBpServiceActorScheduler
			));

		[Rule]
		public Timeout timeout = new Timeout(300000);

		private const long HeartbeatIntervalMs = 5000;

		private const long BlockReportIntervalMs = 10000;

		private readonly Random random = new Random(Runtime.NanoTime());

		// 5 seconds
		// 10 seconds
		[NUnit.Framework.Test]
		public virtual void TestInit()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				NUnit.Framework.Assert.IsTrue(scheduler.IsHeartbeatDue(now));
				NUnit.Framework.Assert.IsTrue(scheduler.IsBlockReportDue());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestScheduleBlockReportImmediate()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				scheduler.ScheduleBlockReport(0);
				NUnit.Framework.Assert.IsTrue(scheduler.resetBlockReportTime);
				Assert.AssertThat(scheduler.nextBlockReportTime, IS.Is(now));
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestScheduleBlockReportDelayed()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				long delayMs = 10;
				scheduler.ScheduleBlockReport(delayMs);
				NUnit.Framework.Assert.IsTrue(scheduler.resetBlockReportTime);
				NUnit.Framework.Assert.IsTrue(scheduler.nextBlockReportTime - now >= 0);
				NUnit.Framework.Assert.IsTrue(scheduler.nextBlockReportTime - (now + delayMs) < 0
					);
			}
		}

		/// <summary>
		/// If resetBlockReportTime is true then the next block report must be scheduled
		/// in the range [now, now + BLOCK_REPORT_INTERVAL_SEC).
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestScheduleNextBlockReport()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				NUnit.Framework.Assert.IsTrue(scheduler.resetBlockReportTime);
				scheduler.ScheduleNextBlockReport();
				NUnit.Framework.Assert.IsTrue(scheduler.nextBlockReportTime - (now + BlockReportIntervalMs
					) < 0);
			}
		}

		/// <summary>
		/// If resetBlockReportTime is false then the next block report must be scheduled
		/// exactly at (now + BLOCK_REPORT_INTERVAL_SEC).
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestScheduleNextBlockReport2()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				scheduler.resetBlockReportTime = false;
				scheduler.ScheduleNextBlockReport();
				Assert.AssertThat(scheduler.nextBlockReportTime, IS.Is(now + BlockReportIntervalMs
					));
			}
		}

		/// <summary>Tests the case when a block report was delayed past its scheduled time.</summary>
		/// <remarks>
		/// Tests the case when a block report was delayed past its scheduled time.
		/// In that case the next block report should not be delayed for a full interval.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestScheduleNextBlockReport3()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				scheduler.resetBlockReportTime = false;
				// Make it look like the block report was scheduled to be sent between 1-3
				// intervals ago but sent just now.
				long blockReportDelay = BlockReportIntervalMs + random.Next(2 * (int)BlockReportIntervalMs
					);
				long origBlockReportTime = now - blockReportDelay;
				scheduler.nextBlockReportTime = origBlockReportTime;
				scheduler.ScheduleNextBlockReport();
				NUnit.Framework.Assert.IsTrue(scheduler.nextBlockReportTime - now < BlockReportIntervalMs
					);
				NUnit.Framework.Assert.IsTrue(((scheduler.nextBlockReportTime - origBlockReportTime
					) % BlockReportIntervalMs) == 0);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestScheduleHeartbeat()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				scheduler.ScheduleNextHeartbeat();
				NUnit.Framework.Assert.IsFalse(scheduler.IsHeartbeatDue(now));
				scheduler.ScheduleHeartbeat();
				NUnit.Framework.Assert.IsTrue(scheduler.IsHeartbeatDue(now));
			}
		}

		/// <summary>Regression test for HDFS-9305.</summary>
		/// <remarks>
		/// Regression test for HDFS-9305.
		/// Delayed processing of a heartbeat can cause a subsequent heartbeat
		/// storm.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestScheduleDelayedHeartbeat()
		{
			foreach (long now in GetTimestamps())
			{
				BPServiceActor.Scheduler scheduler = MakeMockScheduler(now);
				scheduler.ScheduleNextHeartbeat();
				NUnit.Framework.Assert.IsFalse(scheduler.IsHeartbeatDue(now));
				// Simulate a delayed heartbeat e.g. due to slow processing by NN.
				scheduler.nextHeartbeatTime = now - (HeartbeatIntervalMs * 10);
				scheduler.ScheduleNextHeartbeat();
				// Ensure that the next heartbeat is not due immediately.
				NUnit.Framework.Assert.IsFalse(scheduler.IsHeartbeatDue(now));
			}
		}

		private BPServiceActor.Scheduler MakeMockScheduler(long now)
		{
			Log.Info("Using now = " + now);
			BPServiceActor.Scheduler mockScheduler = Org.Mockito.Mockito.Spy(new BPServiceActor.Scheduler
				(HeartbeatIntervalMs, BlockReportIntervalMs));
			Org.Mockito.Mockito.DoReturn(now).When(mockScheduler).MonotonicNow();
			mockScheduler.nextBlockReportTime = now;
			mockScheduler.nextHeartbeatTime = now;
			return mockScheduler;
		}

		internal virtual IList<long> GetTimestamps()
		{
			return Arrays.AsList(0L, long.MinValue, long.MaxValue, long.MaxValue - 1, Math.Abs
				(random.NextLong()), -Math.Abs(random.NextLong()));
		}
		// test boundaries
		// test integer overflow
		// positive random
		// negative random
	}
}
