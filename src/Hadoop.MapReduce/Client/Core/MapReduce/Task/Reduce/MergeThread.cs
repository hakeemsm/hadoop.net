using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	internal abstract class MergeThread<T, K, V> : Sharpen.Thread
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.MergeThread
			));

		private AtomicInteger numPending = new AtomicInteger(0);

		private List<IList<T>> pendingToBeMerged;

		protected internal readonly MergeManagerImpl<K, V> manager;

		private readonly ExceptionReporter reporter;

		private bool closed = false;

		private readonly int mergeFactor;

		public MergeThread(MergeManagerImpl<K, V> manager, int mergeFactor, ExceptionReporter
			 reporter)
		{
			this.pendingToBeMerged = new List<IList<T>>();
			this.manager = manager;
			this.mergeFactor = mergeFactor;
			this.reporter = reporter;
		}

		/// <exception cref="System.Exception"/>
		public virtual void Close()
		{
			lock (this)
			{
				closed = true;
				WaitForMerge();
				Interrupt();
			}
		}

		public virtual void StartMerge(ICollection<T> inputs)
		{
			if (!closed)
			{
				numPending.IncrementAndGet();
				IList<T> toMergeInputs = new AList<T>();
				IEnumerator<T> iter = inputs.GetEnumerator();
				for (int ctr = 0; iter.HasNext() && ctr < mergeFactor; ++ctr)
				{
					toMergeInputs.AddItem(iter.Next());
					iter.Remove();
				}
				Log.Info(GetName() + ": Starting merge with " + toMergeInputs.Count + " segments, while ignoring "
					 + inputs.Count + " segments");
				lock (pendingToBeMerged)
				{
					pendingToBeMerged.AddLast(toMergeInputs);
					Sharpen.Runtime.NotifyAll(pendingToBeMerged);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void WaitForMerge()
		{
			lock (this)
			{
				while (numPending.Get() > 0)
				{
					Sharpen.Runtime.Wait(this);
				}
			}
		}

		public override void Run()
		{
			while (true)
			{
				IList<T> inputs = null;
				try
				{
					// Wait for notification to start the merge...
					lock (pendingToBeMerged)
					{
						while (pendingToBeMerged.Count <= 0)
						{
							Sharpen.Runtime.Wait(pendingToBeMerged);
						}
						// Pickup the inputs to merge.
						inputs = pendingToBeMerged.RemoveFirst();
					}
					// Merge
					Merge(inputs);
				}
				catch (Exception)
				{
					numPending.Set(0);
					return;
				}
				catch (Exception t)
				{
					numPending.Set(0);
					reporter.ReportException(t);
					return;
				}
				finally
				{
					lock (this)
					{
						numPending.DecrementAndGet();
						Sharpen.Runtime.NotifyAll(this);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public abstract void Merge(IList<T> inputs);
	}
}
