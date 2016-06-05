using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// A JUnit test to test
	/// <see cref="ResourceCalculatorPlugin"/>
	/// </summary>
	public class TestResourceCalculatorProcessTree
	{
		public class EmptyProcessTree : ResourceCalculatorProcessTree
		{
			public EmptyProcessTree(string pid)
				: base(pid)
			{
			}

			public override void UpdateProcessTree()
			{
			}

			public override string GetProcessTreeDump()
			{
				return "Empty tree for testing";
			}

			public override long GetRssMemorySize(int age)
			{
				return 0;
			}

			public override long GetCumulativeRssmem(int age)
			{
				return 0;
			}

			public override long GetVirtualMemorySize(int age)
			{
				return 0;
			}

			public override long GetCumulativeVmem(int age)
			{
				return 0;
			}

			public override long GetCumulativeCpuTime()
			{
				return 0;
			}

			public override float GetCpuUsagePercent()
			{
				return CpuTimeTracker.Unavailable;
			}

			public override bool CheckPidPgrpidForMatch()
			{
				return false;
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestCreateInstance()
		{
			ResourceCalculatorProcessTree tree;
			tree = ResourceCalculatorProcessTree.GetResourceCalculatorProcessTree("1", typeof(
				TestResourceCalculatorProcessTree.EmptyProcessTree), new Configuration());
			NUnit.Framework.Assert.IsNotNull(tree);
			Assert.AssertThat(tree, IsInstanceOf.InstanceOf(typeof(TestResourceCalculatorProcessTree.EmptyProcessTree
				)));
		}

		[NUnit.Framework.Test]
		public virtual void TestCreatedInstanceConfigured()
		{
			ResourceCalculatorProcessTree tree;
			Configuration conf = new Configuration();
			tree = ResourceCalculatorProcessTree.GetResourceCalculatorProcessTree("1", typeof(
				TestResourceCalculatorProcessTree.EmptyProcessTree), conf);
			NUnit.Framework.Assert.IsNotNull(tree);
			Assert.AssertThat(tree.GetConf(), IsSame.SameInstance(conf));
		}
	}
}
