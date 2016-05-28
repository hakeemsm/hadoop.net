using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestDatanodeRegister
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestDatanodeRegister));

		private static readonly IPEndPoint InvalidAddr = new IPEndPoint("127.0.0.1", 1);

		private BPServiceActor actor;

		internal NamespaceInfo fakeNsInfo;

		internal DNConf mockDnConf;

		// Invalid address
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			mockDnConf = Org.Mockito.Mockito.Mock<DNConf>();
			Org.Mockito.Mockito.DoReturn(VersionInfo.GetVersion()).When(mockDnConf).GetMinimumNameNodeVersion
				();
			DataNode mockDN = Org.Mockito.Mockito.Mock<DataNode>();
			Org.Mockito.Mockito.DoReturn(true).When(mockDN).ShouldRun();
			Org.Mockito.Mockito.DoReturn(mockDnConf).When(mockDN).GetDnConf();
			BPOfferService mockBPOS = Org.Mockito.Mockito.Mock<BPOfferService>();
			Org.Mockito.Mockito.DoReturn(mockDN).When(mockBPOS).GetDataNode();
			actor = new BPServiceActor(InvalidAddr, mockBPOS);
			fakeNsInfo = Org.Mockito.Mockito.Mock<NamespaceInfo>();
			// Return a a good software version.
			Org.Mockito.Mockito.DoReturn(VersionInfo.GetVersion()).When(fakeNsInfo).GetSoftwareVersion
				();
			// Return a good layout version for now.
			Org.Mockito.Mockito.DoReturn(HdfsConstants.NamenodeLayoutVersion).When(fakeNsInfo
				).GetLayoutVersion();
			DatanodeProtocolClientSideTranslatorPB fakeDnProt = Org.Mockito.Mockito.Mock<DatanodeProtocolClientSideTranslatorPB
				>();
			Org.Mockito.Mockito.When(fakeDnProt.VersionRequest()).ThenReturn(fakeNsInfo);
			actor.SetNameNode(fakeDnProt);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSoftwareVersionDifferences()
		{
			// We expect no exception to be thrown when the software versions match.
			NUnit.Framework.Assert.AreEqual(VersionInfo.GetVersion(), actor.RetrieveNamespaceInfo
				().GetSoftwareVersion());
			// We expect no exception to be thrown when the min NN version is below the
			// reported NN version.
			Org.Mockito.Mockito.DoReturn("4.0.0").When(fakeNsInfo).GetSoftwareVersion();
			Org.Mockito.Mockito.DoReturn("3.0.0").When(mockDnConf).GetMinimumNameNodeVersion(
				);
			NUnit.Framework.Assert.AreEqual("4.0.0", actor.RetrieveNamespaceInfo().GetSoftwareVersion
				());
			// When the NN reports a version that's too low, throw an exception.
			Org.Mockito.Mockito.DoReturn("3.0.0").When(fakeNsInfo).GetSoftwareVersion();
			Org.Mockito.Mockito.DoReturn("4.0.0").When(mockDnConf).GetMinimumNameNodeVersion(
				);
			try
			{
				actor.RetrieveNamespaceInfo();
				NUnit.Framework.Assert.Fail("Should have thrown an exception for NN with too-low version"
					);
			}
			catch (IncorrectVersionException ive)
			{
				GenericTestUtils.AssertExceptionContains("The reported NameNode version is too low"
					, ive);
				Log.Info("Got expected exception", ive);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDifferentLayoutVersions()
		{
			// We expect no exceptions to be thrown when the layout versions match.
			NUnit.Framework.Assert.AreEqual(HdfsConstants.NamenodeLayoutVersion, actor.RetrieveNamespaceInfo
				().GetLayoutVersion());
			// We expect an exception to be thrown when the NN reports a layout version
			// different from that of the DN.
			Org.Mockito.Mockito.DoReturn(HdfsConstants.NamenodeLayoutVersion * 1000).When(fakeNsInfo
				).GetLayoutVersion();
			try
			{
				actor.RetrieveNamespaceInfo();
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("Should not fail to retrieve NS info from DN with different layout version"
					);
			}
		}
	}
}
