using NUnit.Framework;
using NUnit.Framework.Rules;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public abstract class HFSTestCase : HTestCase
	{
		[Rule]
		public MethodRule hdfsTestHelper = new TestHdfsHelper();
	}
}
