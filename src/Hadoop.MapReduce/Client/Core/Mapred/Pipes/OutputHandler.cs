using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>Handles the upward (C++ to Java) messages from the application.</summary>
	internal class OutputHandler<K, V> : UpwardProtocol<K, V>
		where K : WritableComparable
		where V : Writable
	{
		private Reporter reporter;

		private OutputCollector<K, V> collector;

		private float progressValue = 0.0f;

		private bool done = false;

		private Exception exception = null;

		internal RecordReader<FloatWritable, NullWritable> recordReader = null;

		private IDictionary<int, Counters.Counter> registeredCounters = new Dictionary<int
			, Counters.Counter>();

		private string expectedDigest = null;

		private bool digestReceived = false;

		/// <summary>Create a handler that will handle any records output from the application.
		/// 	</summary>
		/// <param name="collector">the "real" collector that takes the output</param>
		/// <param name="reporter">the reporter for reporting progress</param>
		public OutputHandler(OutputCollector<K, V> collector, Reporter reporter, RecordReader
			<FloatWritable, NullWritable> recordReader, string expectedDigest)
		{
			this.reporter = reporter;
			this.collector = collector;
			this.recordReader = recordReader;
			this.expectedDigest = expectedDigest;
		}

		/// <summary>The task output a normal record.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Output(K key, V value)
		{
			collector.Collect(key, value);
		}

		/// <summary>The task output a record with a partition number attached.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void PartitionedOutput(int reduce, K key, V value)
		{
			PipesPartitioner.SetNextPartition(reduce);
			collector.Collect(key, value);
		}

		/// <summary>Update the status message for the task.</summary>
		public virtual void Status(string msg)
		{
			reporter.SetStatus(msg);
		}

		private FloatWritable progressKey = new FloatWritable(0.0f);

		private NullWritable nullValue = NullWritable.Get();

		/// <summary>Update the amount done and call progress on the reporter.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Progress(float progress)
		{
			progressValue = progress;
			reporter.Progress();
			if (recordReader != null)
			{
				progressKey.Set(progress);
				recordReader.Next(progressKey, nullValue);
			}
		}

		/// <summary>The task finished successfully.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Done()
		{
			lock (this)
			{
				done = true;
				Sharpen.Runtime.Notify(this);
			}
		}

		/// <summary>Get the current amount done.</summary>
		/// <returns>a float between 0.0 and 1.0</returns>
		public virtual float GetProgress()
		{
			return progressValue;
		}

		/// <summary>The task failed with an exception.</summary>
		public virtual void Failed(Exception e)
		{
			lock (this)
			{
				exception = e;
				Sharpen.Runtime.Notify(this);
			}
		}

		/// <summary>Wait for the task to finish or abort.</summary>
		/// <returns>did the task finish correctly?</returns>
		/// <exception cref="System.Exception"/>
		public virtual bool WaitForFinish()
		{
			lock (this)
			{
				while (!done && exception == null)
				{
					Sharpen.Runtime.Wait(this);
				}
				if (exception != null)
				{
					throw exception;
				}
				return done;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RegisterCounter(int id, string group, string name)
		{
			Counters.Counter counter = reporter.GetCounter(group, name);
			registeredCounters[id] = counter;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void IncrementCounter(int id, long amount)
		{
			if (id < registeredCounters.Count)
			{
				Counters.Counter counter = registeredCounters[id];
				counter.Increment(amount);
			}
			else
			{
				throw new IOException("Invalid counter with id: " + id);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool Authenticate(string digest)
		{
			lock (this)
			{
				bool success = true;
				if (!expectedDigest.Equals(digest))
				{
					exception = new IOException("Authentication Failed: Expected digest=" + expectedDigest
						 + ", received=" + digestReceived);
					success = false;
				}
				digestReceived = true;
				Sharpen.Runtime.Notify(this);
				return success;
			}
		}

		/// <summary>
		/// This is called by Application and blocks the thread until
		/// authentication response is received.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		internal virtual void WaitForAuthentication()
		{
			lock (this)
			{
				while (digestReceived == false && exception == null)
				{
					Sharpen.Runtime.Wait(this);
				}
				if (exception != null)
				{
					throw new IOException(exception.Message);
				}
			}
		}
	}
}
