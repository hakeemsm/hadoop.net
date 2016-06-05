using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>Manage byte array creation and release.</summary>
	public abstract class ByteArrayManager
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(ByteArrayManager));

		private sealed class _ThreadLocal_39 : ThreadLocal<StringBuilder>
		{
			public _ThreadLocal_39()
			{
			}

			protected override StringBuilder InitialValue()
			{
				return new StringBuilder();
			}
		}

		private static readonly ThreadLocal<StringBuilder> debugMessage = new _ThreadLocal_39
			();

		private static void LogDebugMessage()
		{
			StringBuilder b = debugMessage.Get();
			Log.Debug(b);
			b.Length = 0;
		}

		internal const int MinArrayLength = 32;

		internal static readonly byte[] EmptyByteArray = new byte[] {  };

		/// <returns>
		/// the least power of two greater than or equal to n, i.e. return
		/// the least integer x with x &gt;= n and x a power of two.
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.HadoopIllegalArgumentException">if n &lt;= 0.</exception>
		public static int LeastPowerOfTwo(int n)
		{
			if (n <= 0)
			{
				throw new HadoopIllegalArgumentException("n = " + n + " <= 0");
			}
			int highestOne = int.HighestOneBit(n);
			if (highestOne == n)
			{
				return n;
			}
			// n is a power of two.
			int roundUp = highestOne << 1;
			if (roundUp < 0)
			{
				long overflow = ((long)highestOne) << 1;
				throw new ArithmeticException("Overflow: for n = " + n + ", the least power of two (the least"
					 + " integer x with x >= n and x a power of two) = " + overflow + " > Integer.MAX_VALUE = "
					 + int.MaxValue);
			}
			return roundUp;
		}

		/// <summary>
		/// A counter with a time stamp so that it is reset automatically
		/// if there is no increment for the time period.
		/// </summary>
		internal class Counter
		{
			private readonly long countResetTimePeriodMs;

			private long count = 0L;

			private long timestamp = Time.MonotonicNow();

			internal Counter(long countResetTimePeriodMs)
			{
				this.countResetTimePeriodMs = countResetTimePeriodMs;
			}

			internal virtual long GetCount()
			{
				lock (this)
				{
					return count;
				}
			}

			/// <summary>
			/// Increment the counter, and reset it if there is no increment
			/// for acertain time period.
			/// </summary>
			/// <returns>the new count.</returns>
			internal virtual long Increment()
			{
				lock (this)
				{
					long now = Time.MonotonicNow();
					if (now - timestamp > countResetTimePeriodMs)
					{
						count = 0;
					}
					// reset the counter
					timestamp = now;
					return ++count;
				}
			}
		}

		/// <summary>A map from integers to counters.</summary>
		internal class CounterMap
		{
			/// <seealso cref="Conf.countResetTimePeriodMs"></seealso>
			private readonly long countResetTimePeriodMs;

			private readonly IDictionary<int, ByteArrayManager.Counter> map = new Dictionary<
				int, ByteArrayManager.Counter>();

			private CounterMap(long countResetTimePeriodMs)
			{
				this.countResetTimePeriodMs = countResetTimePeriodMs;
			}

			/// <returns>
			/// the counter for the given key;
			/// and create a new counter if it does not exist.
			/// </returns>
			internal virtual ByteArrayManager.Counter Get(int key, bool createIfNotExist)
			{
				lock (this)
				{
					ByteArrayManager.Counter count = map[key];
					if (count == null && createIfNotExist)
					{
						count = new ByteArrayManager.Counter(countResetTimePeriodMs);
						map[key] = count;
					}
					return count;
				}
			}

			internal virtual void Clear()
			{
				lock (this)
				{
					map.Clear();
				}
			}
		}

		/// <summary>Manage byte arrays with the same fixed length.</summary>
		internal class FixedLengthManager
		{
			private readonly int byteArrayLength;

			private readonly int maxAllocated;

			private readonly Queue<byte[]> freeQueue = new List<byte[]>();

			private int numAllocated = 0;

			internal FixedLengthManager(int arrayLength, int maxAllocated)
			{
				this.byteArrayLength = arrayLength;
				this.maxAllocated = maxAllocated;
			}

			/// <summary>Allocate a byte array.</summary>
			/// <remarks>
			/// Allocate a byte array.
			/// If the number of allocated arrays &gt;= maximum, the current thread is
			/// blocked until the number of allocated arrays drops to below the maximum.
			/// The byte array allocated by this method must be returned for recycling
			/// via the
			/// <see cref="Recycle(byte[])"/>
			/// method.
			/// </remarks>
			/// <exception cref="System.Exception"/>
			internal virtual byte[] Allocate()
			{
				lock (this)
				{
					if (Log.IsDebugEnabled())
					{
						debugMessage.Get().Append(", ").Append(this);
					}
					for (; numAllocated >= maxAllocated; )
					{
						if (Log.IsDebugEnabled())
						{
							debugMessage.Get().Append(": wait ...");
							LogDebugMessage();
						}
						Sharpen.Runtime.Wait(this);
						if (Log.IsDebugEnabled())
						{
							debugMessage.Get().Append("wake up: ").Append(this);
						}
					}
					numAllocated++;
					byte[] array = freeQueue.Poll();
					if (Log.IsDebugEnabled())
					{
						debugMessage.Get().Append(", recycled? ").Append(array != null);
					}
					return array != null ? array : new byte[byteArrayLength];
				}
			}

			/// <summary>
			/// Recycle the given byte array, which must have the same length as the
			/// array length managed by this object.
			/// </summary>
			/// <remarks>
			/// Recycle the given byte array, which must have the same length as the
			/// array length managed by this object.
			/// The byte array may or may not be allocated
			/// by the
			/// <see cref="Allocate()"/>
			/// method.
			/// </remarks>
			internal virtual int Recycle(byte[] array)
			{
				lock (this)
				{
					Preconditions.CheckNotNull(array);
					Preconditions.CheckArgument(array.Length == byteArrayLength);
					if (Log.IsDebugEnabled())
					{
						debugMessage.Get().Append(", ").Append(this);
					}
					Sharpen.Runtime.Notify(this);
					numAllocated--;
					if (numAllocated < 0)
					{
						// it is possible to drop below 0 since
						// some byte arrays may not be created by the allocate() method.
						numAllocated = 0;
					}
					if (freeQueue.Count < maxAllocated - numAllocated)
					{
						if (Log.IsDebugEnabled())
						{
							debugMessage.Get().Append(", freeQueue.offer");
						}
						freeQueue.Offer(array);
					}
					return freeQueue.Count;
				}
			}

			public override string ToString()
			{
				lock (this)
				{
					return "[" + byteArrayLength + ": " + numAllocated + "/" + maxAllocated + ", free="
						 + freeQueue.Count + "]";
				}
			}
		}

		/// <summary>A map from array lengths to byte array managers.</summary>
		internal class ManagerMap
		{
			private readonly int countLimit;

			private readonly IDictionary<int, ByteArrayManager.FixedLengthManager> map = new 
				Dictionary<int, ByteArrayManager.FixedLengthManager>();

			internal ManagerMap(int countLimit)
			{
				this.countLimit = countLimit;
			}

			/// <returns>the manager for the given array length.</returns>
			internal virtual ByteArrayManager.FixedLengthManager Get(int arrayLength, bool createIfNotExist
				)
			{
				lock (this)
				{
					ByteArrayManager.FixedLengthManager manager = map[arrayLength];
					if (manager == null && createIfNotExist)
					{
						manager = new ByteArrayManager.FixedLengthManager(arrayLength, countLimit);
						map[arrayLength] = manager;
					}
					return manager;
				}
			}

			internal virtual void Clear()
			{
				lock (this)
				{
					map.Clear();
				}
			}
		}

		public class Conf
		{
			/// <summary>
			/// The count threshold for each array length so that a manager is created
			/// only after the allocation count exceeds the threshold.
			/// </summary>
			private readonly int countThreshold;

			/// <summary>The maximum number of arrays allowed for each array length.</summary>
			private readonly int countLimit;

			/// <summary>
			/// The time period in milliseconds that the allocation count for each array
			/// length is reset to zero if there is no increment.
			/// </summary>
			private readonly long countResetTimePeriodMs;

			public Conf(int countThreshold, int countLimit, long countResetTimePeriodMs)
			{
				this.countThreshold = countThreshold;
				this.countLimit = countLimit;
				this.countResetTimePeriodMs = countResetTimePeriodMs;
			}
		}

		/// <summary>
		/// Create a byte array for the given length, where the length of
		/// the returned array is larger than or equal to the given length.
		/// </summary>
		/// <remarks>
		/// Create a byte array for the given length, where the length of
		/// the returned array is larger than or equal to the given length.
		/// The current thread may be blocked if some resource is unavailable.
		/// The byte array created by this method must be released
		/// via the
		/// <see cref="Release(byte[])"/>
		/// method.
		/// </remarks>
		/// <returns>a byte array with length larger than or equal to the given length.</returns>
		/// <exception cref="System.Exception"/>
		public abstract byte[] NewByteArray(int size);

		/// <summary>Release the given byte array.</summary>
		/// <remarks>
		/// Release the given byte array.
		/// The byte array may or may not be created
		/// by the
		/// <see cref="NewByteArray(int)"/>
		/// method.
		/// </remarks>
		/// <returns>the number of free array.</returns>
		public abstract int Release(byte[] array);

		public static ByteArrayManager NewInstance(ByteArrayManager.Conf conf)
		{
			return conf == null ? new ByteArrayManager.NewByteArrayWithoutLimit() : new ByteArrayManager.Impl
				(conf);
		}

		/// <summary>A dummy implementation which simply calls new byte[].</summary>
		internal class NewByteArrayWithoutLimit : ByteArrayManager
		{
			/// <exception cref="System.Exception"/>
			public override byte[] NewByteArray(int size)
			{
				return new byte[size];
			}

			public override int Release(byte[] array)
			{
				return 0;
			}
		}

		/// <summary>
		/// Manage byte array allocation and provide a mechanism for recycling the byte
		/// array objects.
		/// </summary>
		internal class Impl : ByteArrayManager
		{
			private readonly ByteArrayManager.Conf conf;

			private readonly ByteArrayManager.CounterMap counters;

			private readonly ByteArrayManager.ManagerMap managers;

			internal Impl(ByteArrayManager.Conf conf)
			{
				this.conf = conf;
				this.counters = new ByteArrayManager.CounterMap(conf.countResetTimePeriodMs);
				this.managers = new ByteArrayManager.ManagerMap(conf.countLimit);
			}

			/// <summary>
			/// Allocate a byte array, where the length of the allocated array
			/// is the least power of two of the given length
			/// unless the given length is less than
			/// <see cref="ByteArrayManager.MinArrayLength"/>
			/// .
			/// In such case, the returned array length is equal to
			/// <see cref="ByteArrayManager.MinArrayLength"/>
			/// .
			/// If the number of allocated arrays exceeds the capacity,
			/// the current thread is blocked until
			/// the number of allocated arrays drops to below the capacity.
			/// The byte array allocated by this method must be returned for recycling
			/// via the
			/// <see cref="Release(byte[])"/>
			/// method.
			/// </summary>
			/// <returns>a byte array with length larger than or equal to the given length.</returns>
			/// <exception cref="System.Exception"/>
			public override byte[] NewByteArray(int arrayLength)
			{
				Preconditions.CheckArgument(arrayLength >= 0);
				if (Log.IsDebugEnabled())
				{
					debugMessage.Get().Append("allocate(").Append(arrayLength).Append(")");
				}
				byte[] array;
				if (arrayLength == 0)
				{
					array = EmptyByteArray;
				}
				else
				{
					int powerOfTwo = arrayLength <= MinArrayLength ? MinArrayLength : LeastPowerOfTwo
						(arrayLength);
					long count = counters.Get(powerOfTwo, true).Increment();
					bool aboveThreshold = count > conf.countThreshold;
					// create a new manager only if the count is above threshold.
					ByteArrayManager.FixedLengthManager manager = managers.Get(powerOfTwo, aboveThreshold
						);
					if (Log.IsDebugEnabled())
					{
						debugMessage.Get().Append(": count=").Append(count).Append(aboveThreshold ? ", aboveThreshold"
							 : ", belowThreshold");
					}
					array = manager != null ? manager.Allocate() : new byte[powerOfTwo];
				}
				if (Log.IsDebugEnabled())
				{
					debugMessage.Get().Append(", return byte[").Append(array.Length).Append("]");
					LogDebugMessage();
				}
				return array;
			}

			/// <summary>Recycle the given byte array.</summary>
			/// <remarks>
			/// Recycle the given byte array.
			/// The byte array may or may not be allocated
			/// by the
			/// <see cref="NewByteArray(int)"/>
			/// method.
			/// This is a non-blocking call.
			/// </remarks>
			public override int Release(byte[] array)
			{
				Preconditions.CheckNotNull(array);
				if (Log.IsDebugEnabled())
				{
					debugMessage.Get().Append("recycle: array.length=").Append(array.Length);
				}
				int freeQueueSize;
				if (array.Length == 0)
				{
					freeQueueSize = -1;
				}
				else
				{
					ByteArrayManager.FixedLengthManager manager = managers.Get(array.Length, false);
					freeQueueSize = manager == null ? -1 : manager.Recycle(array);
				}
				if (Log.IsDebugEnabled())
				{
					debugMessage.Get().Append(", freeQueueSize=").Append(freeQueueSize);
					LogDebugMessage();
				}
				return freeQueueSize;
			}

			internal virtual ByteArrayManager.CounterMap GetCounters()
			{
				return counters;
			}

			internal virtual ByteArrayManager.ManagerMap GetManagers()
			{
				return managers;
			}
		}
	}
}
