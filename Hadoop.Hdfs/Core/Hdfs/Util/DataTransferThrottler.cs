using System;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>a class to throttle the data transfers.</summary>
	/// <remarks>
	/// a class to throttle the data transfers.
	/// This class is thread safe. It can be shared by multiple threads.
	/// The parameter bandwidthPerSec specifies the total bandwidth shared by
	/// threads.
	/// </remarks>
	public class DataTransferThrottler
	{
		private readonly long period;

		private readonly long periodExtension;

		private long bytesPerPeriod;

		private long curPeriodStart;

		private long curReserve;

		private long bytesAlreadyUsed;

		/// <summary>Constructor</summary>
		/// <param name="bandwidthPerSec">bandwidth allowed in bytes per second.</param>
		public DataTransferThrottler(long bandwidthPerSec)
			: this(500, bandwidthPerSec)
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="period">
		/// in milliseconds. Bandwidth is enforced over this
		/// period.
		/// </param>
		/// <param name="bandwidthPerSec">bandwidth allowed in bytes per second.</param>
		public DataTransferThrottler(long period, long bandwidthPerSec)
		{
			// period over which bw is imposed
			// Max period over which bw accumulates.
			// total number of bytes can be sent in each period
			// current period starting time
			// remaining bytes can be sent in the period
			// by default throttling period is 500ms 
			this.curPeriodStart = Time.MonotonicNow();
			this.period = period;
			this.curReserve = this.bytesPerPeriod = bandwidthPerSec * period / 1000;
			this.periodExtension = period * 3;
		}

		/// <returns>current throttle bandwidth in bytes per second.</returns>
		public virtual long GetBandwidth()
		{
			lock (this)
			{
				return bytesPerPeriod * 1000 / period;
			}
		}

		/// <summary>Sets throttle bandwidth.</summary>
		/// <remarks>
		/// Sets throttle bandwidth. This takes affect latest by the end of current
		/// period.
		/// </remarks>
		public virtual void SetBandwidth(long bytesPerSecond)
		{
			lock (this)
			{
				if (bytesPerSecond <= 0)
				{
					throw new ArgumentException(string.Empty + bytesPerSecond);
				}
				bytesPerPeriod = bytesPerSecond * period / 1000;
			}
		}

		/// <summary>
		/// Given the numOfBytes sent/received since last time throttle was called,
		/// make the current thread sleep if I/O rate is too fast
		/// compared to the given bandwidth.
		/// </summary>
		/// <param name="numOfBytes">number of bytes sent/received since last time throttle was called
		/// 	</param>
		public virtual void Throttle(long numOfBytes)
		{
			lock (this)
			{
				Throttle(numOfBytes, null);
			}
		}

		/// <summary>
		/// Given the numOfBytes sent/received since last time throttle was called,
		/// make the current thread sleep if I/O rate is too fast
		/// compared to the given bandwidth.
		/// </summary>
		/// <remarks>
		/// Given the numOfBytes sent/received since last time throttle was called,
		/// make the current thread sleep if I/O rate is too fast
		/// compared to the given bandwidth.  Allows for optional external cancelation.
		/// </remarks>
		/// <param name="numOfBytes">number of bytes sent/received since last time throttle was called
		/// 	</param>
		/// <param name="canceler">optional canceler to check for abort of throttle</param>
		public virtual void Throttle(long numOfBytes, Canceler canceler)
		{
			lock (this)
			{
				if (numOfBytes <= 0)
				{
					return;
				}
				curReserve -= numOfBytes;
				bytesAlreadyUsed += numOfBytes;
				while (curReserve <= 0)
				{
					if (canceler != null && canceler.IsCancelled())
					{
						return;
					}
					long now = Time.MonotonicNow();
					long curPeriodEnd = curPeriodStart + period;
					if (now < curPeriodEnd)
					{
						// Wait for next period so that curReserve can be increased.
						try
						{
							Sharpen.Runtime.Wait(this, curPeriodEnd - now);
						}
						catch (Exception)
						{
							// Abort throttle and reset interrupted status to make sure other
							// interrupt handling higher in the call stack executes.
							Sharpen.Thread.CurrentThread().Interrupt();
							break;
						}
					}
					else
					{
						if (now < (curPeriodStart + periodExtension))
						{
							curPeriodStart = curPeriodEnd;
							curReserve += bytesPerPeriod;
						}
						else
						{
							// discard the prev period. Throttler might not have
							// been used for a long time.
							curPeriodStart = now;
							curReserve = bytesPerPeriod - bytesAlreadyUsed;
						}
					}
				}
				bytesAlreadyUsed -= numOfBytes;
			}
		}
	}
}
