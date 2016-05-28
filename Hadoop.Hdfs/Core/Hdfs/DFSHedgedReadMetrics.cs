using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>The client-side metrics for hedged read feature.</summary>
	/// <remarks>
	/// The client-side metrics for hedged read feature.
	/// This class has a number of metrics variables that are publicly accessible,
	/// we can grab them from client side, like HBase.
	/// </remarks>
	public class DFSHedgedReadMetrics
	{
		public readonly AtomicLong hedgedReadOps = new AtomicLong();

		public readonly AtomicLong hedgedReadOpsWin = new AtomicLong();

		public readonly AtomicLong hedgedReadOpsInCurThread = new AtomicLong();

		public virtual void IncHedgedReadOps()
		{
			hedgedReadOps.IncrementAndGet();
		}

		public virtual void IncHedgedReadOpsInCurThread()
		{
			hedgedReadOpsInCurThread.IncrementAndGet();
		}

		public virtual void IncHedgedReadWins()
		{
			hedgedReadOpsWin.IncrementAndGet();
		}

		public virtual long GetHedgedReadOps()
		{
			return hedgedReadOps;
		}

		public virtual long GetHedgedReadOpsInCurThread()
		{
			return hedgedReadOpsInCurThread;
		}

		public virtual long GetHedgedReadWins()
		{
			return hedgedReadOpsWin;
		}
	}
}
