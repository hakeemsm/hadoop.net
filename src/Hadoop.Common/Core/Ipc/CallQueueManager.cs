using System;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;

using Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Abstracts queue operations for different blocking queues.</summary>
	public class CallQueueManager<E>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Ipc.CallQueueManager
			));

		internal static Type ConvertQueueClass<E>(Type queneClass)
		{
			System.Type elementClass = typeof(E);
			return (Type)queneClass;
		}

		private readonly AtomicReference<BlockingQueue<E>> putRef;

		private readonly AtomicReference<BlockingQueue<E>> takeRef;

		public CallQueueManager(Type backingClass, int maxQueueSize, string @namespace, Configuration
			 conf)
		{
			// Atomic refs point to active callQueue
			// We have two so we can better control swapping
			BlockingQueue<E> bq = CreateCallQueueInstance(backingClass, maxQueueSize, @namespace
				, conf);
			this.putRef = new AtomicReference<BlockingQueue<E>>(bq);
			this.takeRef = new AtomicReference<BlockingQueue<E>>(bq);
			Log.Info("Using callQueue " + backingClass);
		}

		private T CreateCallQueueInstance<T>(int maxLen, string ns, Configuration conf)
			where T : BlockingQueue<E>
		{
			System.Type theClass = typeof(T);
			// Used for custom, configurable callqueues
			try
			{
				Constructor<T> ctor = theClass.GetDeclaredConstructor(typeof(int), typeof(string)
					, typeof(Configuration));
				return ctor.NewInstance(maxLen, ns, conf);
			}
			catch (RuntimeException e)
			{
				throw;
			}
			catch (Exception)
			{
			}
			// Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
			try
			{
				Constructor<T> ctor = theClass.GetDeclaredConstructor(typeof(int));
				return ctor.NewInstance(maxLen);
			}
			catch (RuntimeException e)
			{
				throw;
			}
			catch (Exception)
			{
			}
			// Last attempt
			try
			{
				Constructor<T> ctor = theClass.GetDeclaredConstructor();
				return ctor.NewInstance();
			}
			catch (RuntimeException e)
			{
				throw;
			}
			catch (Exception)
			{
			}
			// Nothing worked
			throw new RuntimeException(theClass.FullName + " could not be constructed.");
		}

		/// <summary>Insert e into the backing queue or block until we can.</summary>
		/// <remarks>
		/// Insert e into the backing queue or block until we can.
		/// If we block and the queue changes on us, we will insert while the
		/// queue is drained.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void Put(E e)
		{
			putRef.Get().Put(e);
		}

		/// <summary>Retrieve an E from the backing queue or block until we can.</summary>
		/// <remarks>
		/// Retrieve an E from the backing queue or block until we can.
		/// Guaranteed to return an element from the current queue.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual E Take()
		{
			E e = null;
			while (e == null)
			{
				e = takeRef.Get().Poll(1000L, TimeUnit.Milliseconds);
			}
			return e;
		}

		public virtual int Size()
		{
			return takeRef.Get().Count;
		}

		/// <summary>
		/// Replaces active queue with the newly requested one and transfers
		/// all calls to the newQ before returning.
		/// </summary>
		public virtual void SwapQueue(Type queueClassToUse, int maxSize, string ns, Configuration
			 conf)
		{
			lock (this)
			{
				BlockingQueue<E> newQ = CreateCallQueueInstance(queueClassToUse, maxSize, ns, conf
					);
				// Our current queue becomes the old queue
				BlockingQueue<E> oldQ = putRef.Get();
				// Swap putRef first: allow blocked puts() to be unblocked
				putRef.Set(newQ);
				// Wait for handlers to drain the oldQ
				while (!QueueIsReallyEmpty(oldQ))
				{
				}
				// Swap takeRef to handle new calls
				takeRef.Set(newQ);
				Log.Info("Old Queue: " + StringRepr(oldQ) + ", " + "Replacement: " + StringRepr(newQ
					));
			}
		}

		/// <summary>Checks if queue is empty by checking at two points in time.</summary>
		/// <remarks>
		/// Checks if queue is empty by checking at two points in time.
		/// This doesn't mean the queue might not fill up at some point later, but
		/// it should decrease the probability that we lose a call this way.
		/// </remarks>
		private bool QueueIsReallyEmpty<_T0>(BlockingQueue<_T0> q)
		{
			bool wasEmpty = q.IsEmpty();
			try
			{
				Thread.Sleep(10);
			}
			catch (Exception)
			{
				return false;
			}
			return q.IsEmpty() && wasEmpty;
		}

		private string StringRepr(object o)
		{
			return o.GetType().FullName + '@' + Extensions.ToHexString(o.GetHashCode(
				));
		}
	}
}
