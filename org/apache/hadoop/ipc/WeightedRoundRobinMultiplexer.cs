using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>
	/// Determines which queue to start reading from, occasionally drawing from
	/// low-priority queues in order to prevent starvation.
	/// </summary>
	/// <remarks>
	/// Determines which queue to start reading from, occasionally drawing from
	/// low-priority queues in order to prevent starvation. Given the pull pattern
	/// [9, 4, 1] for 3 queues:
	/// The cycle is (a minimum of) 9+4+1=14 reads.
	/// Queue 0 is read (at least) 9 times
	/// Queue 1 is read (at least) 4 times
	/// Queue 2 is read (at least) 1 time
	/// Repeat
	/// There may be more reads than the minimum due to race conditions. This is
	/// allowed by design for performance reasons.
	/// </remarks>
	public class WeightedRoundRobinMultiplexer : org.apache.hadoop.ipc.RpcMultiplexer
	{
		public const string IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY = "faircallqueue.multiplexer.weights";

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.WeightedRoundRobinMultiplexer
			)));

		private readonly int numQueues;

		private readonly java.util.concurrent.atomic.AtomicInteger currentQueueIndex;

		private readonly java.util.concurrent.atomic.AtomicInteger requestsLeft;

		private int[] queueWeights;

		public WeightedRoundRobinMultiplexer(int aNumQueues, string ns, org.apache.hadoop.conf.Configuration
			 conf)
		{
			// Config keys
			// The number of queues under our provisioning
			// Current queue we're serving
			// Number of requests left for this queue
			// The weights for each queue
			if (aNumQueues <= 0)
			{
				throw new System.ArgumentException("Requested queues (" + aNumQueues + ") must be greater than zero."
					);
			}
			this.numQueues = aNumQueues;
			this.queueWeights = conf.getInts(ns + "." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY);
			if (this.queueWeights.Length == 0)
			{
				this.queueWeights = getDefaultQueueWeights(this.numQueues);
			}
			else
			{
				if (this.queueWeights.Length != this.numQueues)
				{
					throw new System.ArgumentException(ns + "." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY + 
						" must specify exactly " + this.numQueues + " weights: one for each priority level."
						);
				}
			}
			this.currentQueueIndex = new java.util.concurrent.atomic.AtomicInteger(0);
			this.requestsLeft = new java.util.concurrent.atomic.AtomicInteger(this.queueWeights
				[0]);
			LOG.info("WeightedRoundRobinMultiplexer is being used.");
		}

		/// <summary>Creates default weights for each queue.</summary>
		/// <remarks>Creates default weights for each queue. The weights are 2^N.</remarks>
		private int[] getDefaultQueueWeights(int aNumQueues)
		{
			int[] weights = new int[aNumQueues];
			int weight = 1;
			// Start low
			for (int i = aNumQueues - 1; i >= 0; i--)
			{
				// Start at lowest queue
				weights[i] = weight;
				weight *= 2;
			}
			// Double every iteration
			return weights;
		}

		/// <summary>Move to the next queue.</summary>
		private void moveToNextQueue()
		{
			int thisIdx = this.currentQueueIndex.get();
			// Wrap to fit in our bounds
			int nextIdx = (thisIdx + 1) % this.numQueues;
			// Set to next index: once this is called, requests will start being
			// drawn from nextIdx, but requestsLeft will continue to decrement into
			// the negatives
			this.currentQueueIndex.set(nextIdx);
			// Finally, reset requestsLeft. This will enable moveToNextQueue to be
			// called again, for the new currentQueueIndex
			this.requestsLeft.set(this.queueWeights[nextIdx]);
		}

		/// <summary>
		/// Advances the index, which will change the current index
		/// if called enough times.
		/// </summary>
		private void advanceIndex()
		{
			// Since we did read, we should decrement
			int requestsLeftVal = this.requestsLeft.decrementAndGet();
			// Strict compare with zero (instead of inequality) so that if another
			// thread decrements requestsLeft, only one thread will be responsible
			// for advancing currentQueueIndex
			if (requestsLeftVal == 0)
			{
				// This is guaranteed to be called exactly once per currentQueueIndex
				this.moveToNextQueue();
			}
		}

		/// <summary>Gets the current index.</summary>
		/// <remarks>
		/// Gets the current index. Should be accompanied by a call to
		/// advanceIndex at some point.
		/// </remarks>
		private int getCurrentIndex()
		{
			return this.currentQueueIndex.get();
		}

		/// <summary>Use the mux by getting and advancing index.</summary>
		public virtual int getAndAdvanceCurrentIndex()
		{
			int idx = this.getCurrentIndex();
			this.advanceIndex();
			return idx;
		}
	}
}
