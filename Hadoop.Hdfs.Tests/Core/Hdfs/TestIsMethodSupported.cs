using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Ipc.ProtocolPB;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.ProtocolPB;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tools.ProtocolPB;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Test cases to verify that client side translators correctly implement the
	/// isMethodSupported method in ProtocolMetaInterface.
	/// </summary>
	public class TestIsMethodSupported
	{
		private static MiniDFSCluster cluster = null;

		private static readonly HdfsConfiguration conf = new HdfsConfiguration();

		private static IPEndPoint nnAddress = null;

		private static IPEndPoint dnAddress = null;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			cluster = (new MiniDFSCluster.Builder(conf)).NumDataNodes(1).Build();
			nnAddress = cluster.GetNameNode().GetNameNodeAddress();
			DataNode dn = cluster.GetDataNodes()[0];
			dnAddress = new IPEndPoint(dn.GetDatanodeId().GetIpAddr(), dn.GetIpcPort());
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNamenodeProtocol()
		{
			NamenodeProtocol np = NameNodeProxies.CreateNonHAProxy<NamenodeProtocol>(conf, nnAddress
				, UserGroupInformation.GetCurrentUser(), true).GetProxy();
			bool exists = RpcClientUtil.IsMethodSupported(np, typeof(NamenodeProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(NamenodeProtocolPB)), "rollEditLog"
				);
			NUnit.Framework.Assert.IsTrue(exists);
			exists = RpcClientUtil.IsMethodSupported(np, typeof(NamenodeProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(NamenodeProtocolPB)), "bogusMethod"
				);
			NUnit.Framework.Assert.IsFalse(exists);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDatanodeProtocol()
		{
			DatanodeProtocolClientSideTranslatorPB translator = new DatanodeProtocolClientSideTranslatorPB
				(nnAddress, conf);
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("sendHeartbeat"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientDatanodeProtocol()
		{
			ClientDatanodeProtocolTranslatorPB translator = new ClientDatanodeProtocolTranslatorPB
				(nnAddress, UserGroupInformation.GetCurrentUser(), conf, NetUtils.GetDefaultSocketFactory
				(conf));
			//Namenode doesn't implement ClientDatanodeProtocol
			NUnit.Framework.Assert.IsFalse(translator.IsMethodSupported("refreshNamenodes"));
			translator = new ClientDatanodeProtocolTranslatorPB(dnAddress, UserGroupInformation
				.GetCurrentUser(), conf, NetUtils.GetDefaultSocketFactory(conf));
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("refreshNamenodes"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientNamenodeProtocol()
		{
			ClientProtocol cp = NameNodeProxies.CreateNonHAProxy<ClientProtocol>(conf, nnAddress
				, UserGroupInformation.GetCurrentUser(), true).GetProxy();
			RpcClientUtil.IsMethodSupported(cp, typeof(ClientNamenodeProtocolPB), RPC.RpcKind
				.RpcProtocolBuffer, RPC.GetProtocolVersion(typeof(ClientNamenodeProtocolPB)), "mkdirs"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TesJournalProtocol()
		{
			JournalProtocolTranslatorPB translator = (JournalProtocolTranslatorPB)NameNodeProxies
				.CreateNonHAProxy<JournalProtocol>(conf, nnAddress, UserGroupInformation.GetCurrentUser
				(), true).GetProxy();
			//Nameode doesn't implement JournalProtocol
			NUnit.Framework.Assert.IsFalse(translator.IsMethodSupported("startLogSegment"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestInterDatanodeProtocol()
		{
			InterDatanodeProtocolTranslatorPB translator = new InterDatanodeProtocolTranslatorPB
				(nnAddress, UserGroupInformation.GetCurrentUser(), conf, NetUtils.GetDefaultSocketFactory
				(conf), 0);
			//Not supported at namenode
			NUnit.Framework.Assert.IsFalse(translator.IsMethodSupported("initReplicaRecovery"
				));
			translator = new InterDatanodeProtocolTranslatorPB(dnAddress, UserGroupInformation
				.GetCurrentUser(), conf, NetUtils.GetDefaultSocketFactory(conf), 0);
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("initReplicaRecovery")
				);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetUserMappingsProtocol()
		{
			GetUserMappingsProtocolClientSideTranslatorPB translator = (GetUserMappingsProtocolClientSideTranslatorPB
				)NameNodeProxies.CreateNonHAProxy<GetUserMappingsProtocol>(conf, nnAddress, UserGroupInformation
				.GetCurrentUser(), true).GetProxy();
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("getGroupsForUser"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshAuthorizationPolicyProtocol()
		{
			RefreshAuthorizationPolicyProtocolClientSideTranslatorPB translator = (RefreshAuthorizationPolicyProtocolClientSideTranslatorPB
				)NameNodeProxies.CreateNonHAProxy<RefreshAuthorizationPolicyProtocol>(conf, nnAddress
				, UserGroupInformation.GetCurrentUser(), true).GetProxy();
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("refreshServiceAcl"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshUserMappingsProtocol()
		{
			RefreshUserMappingsProtocolClientSideTranslatorPB translator = (RefreshUserMappingsProtocolClientSideTranslatorPB
				)NameNodeProxies.CreateNonHAProxy<RefreshUserMappingsProtocol>(conf, nnAddress, 
				UserGroupInformation.GetCurrentUser(), true).GetProxy();
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("refreshUserToGroupsMappings"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshCallQueueProtocol()
		{
			RefreshCallQueueProtocolClientSideTranslatorPB translator = (RefreshCallQueueProtocolClientSideTranslatorPB
				)NameNodeProxies.CreateNonHAProxy<RefreshCallQueueProtocol>(conf, nnAddress, UserGroupInformation
				.GetCurrentUser(), true).GetProxy();
			NUnit.Framework.Assert.IsTrue(translator.IsMethodSupported("refreshCallQueue"));
		}
	}
}
