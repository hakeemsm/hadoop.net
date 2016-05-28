using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestDeduplicationMap
	{
		[NUnit.Framework.Test]
		public virtual void TestDeduplicationMap()
		{
			FSImageFormatProtobuf.SaverContext.DeduplicationMap<string> m = FSImageFormatProtobuf.SaverContext.DeduplicationMap
				.NewMap();
			NUnit.Framework.Assert.AreEqual(1, m.GetId("1"));
			NUnit.Framework.Assert.AreEqual(2, m.GetId("2"));
			NUnit.Framework.Assert.AreEqual(3, m.GetId("3"));
			NUnit.Framework.Assert.AreEqual(1, m.GetId("1"));
			NUnit.Framework.Assert.AreEqual(2, m.GetId("2"));
			NUnit.Framework.Assert.AreEqual(3, m.GetId("3"));
		}
	}
}
