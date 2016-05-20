using Sharpen;

namespace org.apache.hadoop.metrics2.impl
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
			void consume(T @object);
		}

		private readonly T[] data;

		private int head;

		private int tail;

		private int size;

		private java.lang.Thread currentConsumer = null;

		internal SinkQueue(int capacity)
		{
			// A fixed size circular buffer to minimize garbage
			// head position
			// tail position
			// number of elements
			this.data = (T[])new object[System.Math.max(1, capacity)];
			head = tail = size = 0;
		}

		internal virtual bool enqueue(T e)
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
				Sharpen.Runtime.notify(this);
				return true;
			}
		}

		/// <summary>
		/// Consume one element, will block if queue is empty
		/// Only one consumer at a time is allowed
		/// </summary>
		/// <param name="consumer">the consumer callback object</param>
		/// <exception cref="System.Exception"/>
		internal virtual void consume(org.apache.hadoop.metrics2.impl.SinkQueue.Consumer<
			T> consumer)
		{
			T e = waitForData();
			try
			{
				consumer.consume(e);
				// can take forever
				_dequeue();
			}
			finally
			{
				clearConsumerLock();
			}
		}

		/// <summary>Consume all the elements, will block if queue is empty</summary>
		/// <param name="consumer">the consumer callback object</param>
		/// <exception cref="System.Exception"/>
		internal virtual void consumeAll(org.apache.hadoop.metrics2.impl.SinkQueue.Consumer
			<T> consumer)
		{
			waitForData();
			try
			{
				for (int i = size(); i-- > 0; )
				{
					consumer.consume(front());
					// can take forever
					_dequeue();
				}
			}
			finally
			{
				clearConsumerLock();
			}
		}

		/// <summary>Dequeue one element from head of the queue, will block if queue is empty
		/// 	</summary>
		/// <returns>the first element</returns>
		/// <exception cref="System.Exception"/>
		internal virtual T dequeue()
		{
			lock (this)
			{
				checkConsumer();
				while (0 == size)
				{
					Sharpen.Runtime.wait(this);
				}
				return _dequeue();
			}
		}

		/// <exception cref="System.Exception"/>
		private T waitForData()
		{
			lock (this)
			{
				checkConsumer();
				while (0 == size)
				{
					Sharpen.Runtime.wait(this);
				}
				setConsumerLock();
				return front();
			}
		}

		private void checkConsumer()
		{
			lock (this)
			{
				if (currentConsumer != null)
				{
					throw new java.util.ConcurrentModificationException("The " + currentConsumer.getName
						() + " thread is consuming the queue.");
				}
			}
		}

		private void setConsumerLock()
		{
			lock (this)
			{
				currentConsumer = java.lang.Thread.currentThread();
			}
		}

		private void clearConsumerLock()
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
					throw new System.InvalidOperationException("Size must > 0 here.");
				}
				--size;
				head = (head + 1) % data.Length;
				T ret = data[head];
				data[head] = null;
				// hint to gc
				return ret;
			}
		}

		internal virtual T front()
		{
			lock (this)
			{
				return data[(head + 1) % data.Length];
			}
		}

		internal virtual T back()
		{
			lock (this)
			{
				return data[tail];
			}
		}

		internal virtual void clear()
		{
			lock (this)
			{
				checkConsumer();
				for (int i = data.Length; i-- > 0; )
				{
					data[i] = null;
				}
				size = 0;
			}
		}

		internal virtual int size()
		{
			lock (this)
			{
				return size;
			}
		}

		internal virtual int capacity()
		{
			return data.Length;
		}
	}
}
