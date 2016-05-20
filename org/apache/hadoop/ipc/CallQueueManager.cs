using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Abstracts queue operations for different blocking queues.</summary>
	public class CallQueueManager<E>
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.CallQueueManager
			)));

		internal static java.lang.Class convertQueueClass<E>(java.lang.Class queneClass)
		{
			System.Type elementClass = typeof(E);
			return (java.lang.Class)queneClass;
		}

		private readonly java.util.concurrent.atomic.AtomicReference<java.util.concurrent.BlockingQueue
			<E>> putRef;

		private readonly java.util.concurrent.atomic.AtomicReference<java.util.concurrent.BlockingQueue
			<E>> takeRef;

		public CallQueueManager(java.lang.Class backingClass, int maxQueueSize, string @namespace
			, org.apache.hadoop.conf.Configuration conf)
		{
			// Atomic refs point to active callQueue
			// We have two so we can better control swapping
			java.util.concurrent.BlockingQueue<E> bq = createCallQueueInstance(backingClass, 
				maxQueueSize, @namespace, conf);
			this.putRef = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.BlockingQueue
				<E>>(bq);
			this.takeRef = new java.util.concurrent.atomic.AtomicReference<java.util.concurrent.BlockingQueue
				<E>>(bq);
			LOG.info("Using callQueue " + backingClass);
		}

		private T createCallQueueInstance<T>(int maxLen, string ns, org.apache.hadoop.conf.Configuration
			 conf)
			where T : java.util.concurrent.BlockingQueue<E>
		{
			System.Type theClass = typeof(T);
			// Used for custom, configurable callqueues
			try
			{
				java.lang.reflect.Constructor<T> ctor = theClass.getDeclaredConstructor(Sharpen.Runtime.getClassForType
					(typeof(int)), Sharpen.Runtime.getClassForType(typeof(string)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.conf.Configuration)));
				return ctor.newInstance(maxLen, ns, conf);
			}
			catch (System.Exception e)
			{
				throw;
			}
			catch (System.Exception)
			{
			}
			// Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
			try
			{
				java.lang.reflect.Constructor<T> ctor = theClass.getDeclaredConstructor(Sharpen.Runtime.getClassForType
					(typeof(int)));
				return ctor.newInstance(maxLen);
			}
			catch (System.Exception e)
			{
				throw;
			}
			catch (System.Exception)
			{
			}
			// Last attempt
			try
			{
				java.lang.reflect.Constructor<T> ctor = theClass.getDeclaredConstructor();
				return ctor.newInstance();
			}
			catch (System.Exception e)
			{
				throw;
			}
			catch (System.Exception)
			{
			}
			// Nothing worked
			throw new System.Exception(theClass.getName() + " could not be constructed.");
		}

		/// <summary>Insert e into the backing queue or block until we can.</summary>
		/// <remarks>
		/// Insert e into the backing queue or block until we can.
		/// If we block and the queue changes on us, we will insert while the
		/// queue is drained.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void put(E e)
		{
			putRef.get().put(e);
		}

		/// <summary>Retrieve an E from the backing queue or block until we can.</summary>
		/// <remarks>
		/// Retrieve an E from the backing queue or block until we can.
		/// Guaranteed to return an element from the current queue.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual E take()
		{
			E e = null;
			while (e == null)
			{
				e = takeRef.get().poll(1000L, java.util.concurrent.TimeUnit.MILLISECONDS);
			}
			return e;
		}

		public virtual int size()
		{
			return takeRef.get().Count;
		}

		/// <summary>
		/// Replaces active queue with the newly requested one and transfers
		/// all calls to the newQ before returning.
		/// </summary>
		public virtual void swapQueue(java.lang.Class queueClassToUse, int maxSize, string
			 ns, org.apache.hadoop.conf.Configuration conf)
		{
			lock (this)
			{
				java.util.concurrent.BlockingQueue<E> newQ = createCallQueueInstance(queueClassToUse
					, maxSize, ns, conf);
				// Our current queue becomes the old queue
				java.util.concurrent.BlockingQueue<E> oldQ = putRef.get();
				// Swap putRef first: allow blocked puts() to be unblocked
				putRef.set(newQ);
				// Wait for handlers to drain the oldQ
				while (!queueIsReallyEmpty(oldQ))
				{
				}
				// Swap takeRef to handle new calls
				takeRef.set(newQ);
				LOG.info("Old Queue: " + stringRepr(oldQ) + ", " + "Replacement: " + stringRepr(newQ
					));
			}
		}

		/// <summary>Checks if queue is empty by checking at two points in time.</summary>
		/// <remarks>
		/// Checks if queue is empty by checking at two points in time.
		/// This doesn't mean the queue might not fill up at some point later, but
		/// it should decrease the probability that we lose a call this way.
		/// </remarks>
		private bool queueIsReallyEmpty<_T0>(java.util.concurrent.BlockingQueue<_T0> q)
		{
			bool wasEmpty = q.isEmpty();
			try
			{
				java.lang.Thread.sleep(10);
			}
			catch (System.Exception)
			{
				return false;
			}
			return q.isEmpty() && wasEmpty;
		}

		private string stringRepr(object o)
		{
			return Sharpen.Runtime.getClassForObject(o).getName() + '@' + int.toHexString(o.GetHashCode
				());
		}
	}
}
