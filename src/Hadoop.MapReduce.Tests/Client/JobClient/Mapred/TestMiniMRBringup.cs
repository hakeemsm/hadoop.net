using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A Unit-test to test bringup and shutdown of Mini Map-Reduce Cluster.</summary>
	public class TestMiniMRBringup : TestCase
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBringUp()
		{
			MiniMRCluster mr = null;
			try
			{
				mr = new MiniMRCluster(1, "local", 1);
			}
			finally
			{
				if (mr != null)
				{
					mr.Shutdown();
				}
			}
		}
	}
}
