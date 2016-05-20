using Sharpen;

namespace org.apache.hadoop.ha
{
	/// <summary>A fake shared resource, for use in automatic failover testing.</summary>
	/// <remarks>
	/// A fake shared resource, for use in automatic failover testing.
	/// This simulates a real shared resource like a shared edit log.
	/// When the
	/// <see cref="DummyHAService"/>
	/// instances change state or get
	/// fenced, they notify the shared resource, which asserts that
	/// we never have two HA services who think they're holding the
	/// resource at the same time.
	/// </remarks>
	public class DummySharedResource
	{
		private org.apache.hadoop.ha.DummyHAService holder = null;

		private int violations = 0;

		public virtual void take(org.apache.hadoop.ha.DummyHAService newHolder)
		{
			lock (this)
			{
				if (holder == null || holder == newHolder)
				{
					holder = newHolder;
				}
				else
				{
					violations++;
					throw new System.InvalidOperationException("already held by: " + holder);
				}
			}
		}

		public virtual void release(org.apache.hadoop.ha.DummyHAService oldHolder)
		{
			lock (this)
			{
				if (holder == oldHolder)
				{
					holder = null;
				}
			}
		}

		public virtual void assertNoViolations()
		{
			lock (this)
			{
				NUnit.Framework.Assert.AreEqual(0, violations);
			}
		}
	}
}
