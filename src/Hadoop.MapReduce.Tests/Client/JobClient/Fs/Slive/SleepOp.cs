using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Operation which sleeps for a given number of milliseconds according to the
	/// config given, and reports on the sleep time overall
	/// </summary>
	internal class SleepOp : Operation
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Slive.SleepOp
			));

		internal SleepOp(ConfigExtractor cfg, Random rnd)
			: base(typeof(Org.Apache.Hadoop.FS.Slive.SleepOp).Name, cfg, rnd)
		{
		}

		protected internal virtual long GetSleepTime(Range<long> sleepTime)
		{
			long sleepMs = Range.BetweenPositive(GetRandom(), sleepTime);
			return sleepMs;
		}

		/// <summary>Sleep for a random amount of time between a given positive range</summary>
		/// <param name="sleepTime">positive long range for times to choose</param>
		/// <returns>output data on operation</returns>
		internal virtual IList<OperationOutput> Run(Range<long> sleepTime)
		{
			IList<OperationOutput> @out = base.Run(null);
			try
			{
				if (sleepTime != null)
				{
					long sleepMs = GetSleepTime(sleepTime);
					long startTime = Timer.Now();
					Sleep(sleepMs);
					long elapsedTime = Timer.Elapsed(startTime);
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.OkTimeTaken, elapsedTime));
					@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
						.Successes, 1L));
				}
			}
			catch (Exception e)
			{
				@out.AddItem(new OperationOutput(OperationOutput.OutputType.Long, GetType(), ReportWriter
					.Failures, 1L));
				Log.Warn("Error with sleeping", e);
			}
			return @out;
		}

		internal override IList<OperationOutput> Run(FileSystem fs)
		{
			// Operation
			Range<long> sleepTime = GetConfig().GetSleepRange();
			return Run(sleepTime);
		}

		/// <summary>Sleeps the current thread for X milliseconds</summary>
		/// <param name="ms">milliseconds to sleep for</param>
		/// <exception cref="System.Exception"/>
		private void Sleep(long ms)
		{
			if (ms <= 0)
			{
				return;
			}
			Sharpen.Thread.Sleep(ms);
		}
	}
}
