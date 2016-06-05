using System;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>
	/// A half-blocking (nonblocking for producers, blocking for consumers) queue
	/// for metrics sinks.
	/// </summary>
	/// <remarks>
	/// A half-blocking (nonblocking for producers, blocking for consumers) queue
	/// for metrics sinks.
	/// New elements are dropped when the queue is full to preserve "interesting"
	/// elements at the onset of queue filling events
	/// </remarks>
	internal class SinkQueue<T>
	{
		internal interface Consumer<T>
		{
			/// <exception cref="System.Exception"/>
			void Consume(T @object);
		}

		private readonly T[] data;

		private int head;

		private int tail;

		private int size;

		private Thread currentConsumer = null;

		internal SinkQueue(int capacity)
		{
			// A fixed size circular buffer to minimize garbage
			// head position
			// tail position
			// number of elements
			this.data = (T[])new object[Math.Max(1, capacity)];
			head = tail = size = 0;
		}

		internal virtual bool Enqueue(T e)
		{
			lock (this)
			{
				if (data.Length == size)
				{
					return false;
				}
				++size;
				tail = (tail + 1) % data.Length;
				data[tail] = e;
				Runtime.Notify(this);
				return true;
			}
		}

		/// <summary>
		/// Consume one element, will block if queue is empty
		/// Only one consumer at a time is allowed
		/// </summary>
		/// <param name="consumer">the consumer callback object</param>
		/// <exception cref="System.Exception"/>
		internal virtual void Consume(SinkQueue.Consumer<T> consumer)
		{
			T e = WaitForData();
			try
			{
				consumer.Consume(e);
				// can take forever
				_dequeue();
			}
			finally
			{
				ClearConsumerLock();
			}
		}

		/// <summary>Consume all the elements, will block if queue is empty</summary>
		/// <param name="consumer">the consumer callback object</param>
		/// <exception cref="System.Exception"/>
		internal virtual void ConsumeAll(SinkQueue.Consumer<T> consumer)
		{
			WaitForData();
			try
			{
				for (int i = Size(); i-- > 0; )
				{
					consumer.Consume(Front());
					// can take forever
					_dequeue();
				}
			}
			finally
			{
				ClearConsumerLock();
			}
		}

		/// <summary>Dequeue one element from head of the queue, will block if queue is empty
		/// 	</summary>
		/// <returns>the first element</returns>
		/// <exception cref="System.Exception"/>
		internal virtual T Dequeue()
		{
			lock (this)
			{
				CheckConsumer();
				while (0 == size)
				{
					Runtime.Wait(this);
				}
				return _dequeue();
			}
		}

		/// <exception cref="System.Exception"/>
		private T WaitForData()
		{
			lock (this)
			{
				CheckConsumer();
				while (0 == size)
				{
					Runtime.Wait(this);
				}
				SetConsumerLock();
				return Front();
			}
		}

		private void CheckConsumer()
		{
			lock (this)
			{
				if (currentConsumer != null)
				{
					throw new ConcurrentModificationException("The " + currentConsumer.GetName() + " thread is consuming the queue."
						);
				}
			}
		}

		private void SetConsumerLock()
		{
			lock (this)
			{
				currentConsumer = Thread.CurrentThread();
			}
		}

		private void ClearConsumerLock()
		{
			lock (this)
			{
				currentConsumer = null;
			}
		}

		private T _dequeue()
		{
			lock (this)
			{
				if (0 == size)
				{
					throw new InvalidOperationException("Size must > 0 here.");
				}
				--size;
				head = (head + 1) % data.Length;
				T ret = data[head];
				data[head] = null;
				// hint to gc
				return ret;
			}
		}

		internal virtual T Front()
		{
			lock (this)
			{
				return data[(head + 1) % data.Length];
			}
		}

		internal virtual T Back()
		{
			lock (this)
			{
				return data[tail];
			}
		}

		internal virtual void Clear()
		{
			lock (this)
			{
				CheckConsumer();
				for (int i = data.Length; i-- > 0; )
				{
					data[i] = null;
				}
				size = 0;
			}
		}

		internal virtual int Size()
		{
			lock (this)
			{
				return size;
			}
		}

		internal virtual int Capacity()
		{
			return data.Length;
		}
	}
}
