using System;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.HA
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
		private DummyHAService holder = null;

		private int violations = 0;

		public virtual void Take(DummyHAService newHolder)
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
					throw new InvalidOperationException("already held by: " + holder);
				}
			}
		}

		public virtual void Release(DummyHAService oldHolder)
		{
			lock (this)
			{
				if (holder == oldHolder)
				{
					holder = null;
				}
			}
		}

		public virtual void AssertNoViolations()
		{
			lock (this)
			{
				NUnit.Framework.Assert.AreEqual(0, violations);
			}
		}
	}
}
