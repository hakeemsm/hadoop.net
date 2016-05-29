using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class TestConverterUtils
	{
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestConvertUrlWithNoPort()
		{
			Path expectedPath = new Path("hdfs://foo.com");
			URL url = ConverterUtils.GetYarnUrlFromPath(expectedPath);
			Path actualPath = ConverterUtils.GetPathFromYarnURL(url);
			NUnit.Framework.Assert.AreEqual(expectedPath, actualPath);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestConvertUrlWithUserinfo()
		{
			Path expectedPath = new Path("foo://username:password@example.com:8042");
			URL url = ConverterUtils.GetYarnUrlFromPath(expectedPath);
			Path actualPath = ConverterUtils.GetPathFromYarnURL(url);
			NUnit.Framework.Assert.AreEqual(expectedPath, actualPath);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerId()
		{
			ContainerId id = Org.Apache.Hadoop.Yarn.Api.TestContainerId.NewContainerId(0, 0, 
				0, 0);
			string cid = ConverterUtils.ToString(id);
			NUnit.Framework.Assert.AreEqual("container_0_0000_00_000000", cid);
			ContainerId gen = ConverterUtils.ToContainerId(cid);
			NUnit.Framework.Assert.AreEqual(gen, id);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerIdWithEpoch()
		{
			ContainerId id = Org.Apache.Hadoop.Yarn.Api.TestContainerId.NewContainerId(0, 0, 
				0, 25645811);
			string cid = ConverterUtils.ToString(id);
			NUnit.Framework.Assert.AreEqual("container_0_0000_00_25645811", cid);
			ContainerId gen = ConverterUtils.ToContainerId(cid);
			NUnit.Framework.Assert.AreEqual(gen.ToString(), id.ToString());
			long ts = Runtime.CurrentTimeMillis();
			ContainerId id2 = Org.Apache.Hadoop.Yarn.Api.TestContainerId.NewContainerId(36473
				, 4365472, ts, 4298334883325L);
			string cid2 = ConverterUtils.ToString(id2);
			NUnit.Framework.Assert.AreEqual("container_e03_" + ts + "_36473_4365472_999799999997"
				, cid2);
			ContainerId gen2 = ConverterUtils.ToContainerId(cid2);
			NUnit.Framework.Assert.AreEqual(gen2.ToString(), id2.ToString());
			ContainerId id3 = Org.Apache.Hadoop.Yarn.Api.TestContainerId.NewContainerId(36473
				, 4365472, ts, 844424930131965L);
			string cid3 = ConverterUtils.ToString(id3);
			NUnit.Framework.Assert.AreEqual("container_e767_" + ts + "_36473_4365472_1099511627773"
				, cid3);
			ContainerId gen3 = ConverterUtils.ToContainerId(cid3);
			NUnit.Framework.Assert.AreEqual(gen3.ToString(), id3.ToString());
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerIdNull()
		{
			NUnit.Framework.Assert.IsNull(ConverterUtils.ToString((ContainerId)null));
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeIdWithDefaultPort()
		{
			NodeId nid;
			nid = ConverterUtils.ToNodeIdWithDefaultPort("node:10");
			NUnit.Framework.Assert.AreEqual(nid.GetPort(), 10);
			NUnit.Framework.Assert.AreEqual(nid.GetHost(), "node");
			nid = ConverterUtils.ToNodeIdWithDefaultPort("node");
			NUnit.Framework.Assert.AreEqual(nid.GetPort(), 0);
			NUnit.Framework.Assert.AreEqual(nid.GetHost(), "node");
		}

		public virtual void TestInvalidContainerId()
		{
			ConverterUtils.ToContainerId("container_e20_1423221031460_0003_01");
		}

		public virtual void TestInvalidAppattemptId()
		{
			ConverterUtils.ToApplicationAttemptId("appattempt_1423221031460");
		}

		public virtual void TestApplicationId()
		{
			ConverterUtils.ToApplicationId("application_1423221031460");
		}
	}
}
