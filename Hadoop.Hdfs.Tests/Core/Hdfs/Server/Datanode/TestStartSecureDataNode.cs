using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// This test starts a 1 NameNode 1 DataNode MiniDFSCluster with
	/// kerberos authentication enabled using user-specified KDC,
	/// principals, and keytabs.
	/// </summary>
	/// <remarks>
	/// This test starts a 1 NameNode 1 DataNode MiniDFSCluster with
	/// kerberos authentication enabled using user-specified KDC,
	/// principals, and keytabs.
	/// A secure DataNode has to be started by root, so this test needs to
	/// be run by root.
	/// To run, users must specify the following system properties:
	/// externalKdc=true
	/// java.security.krb5.conf
	/// dfs.namenode.kerberos.principal
	/// dfs.namenode.kerberos.internal.spnego.principal
	/// dfs.namenode.keytab.file
	/// dfs.datanode.kerberos.principal
	/// dfs.datanode.keytab.file
	/// </remarks>
	public class TestStartSecureDataNode
	{
		private const int NumOfDatanodes = 1;

		[SetUp]
		public virtual void TestExternalKdcRunning()
		{
			// Tests are skipped if external KDC is not running.
			Assume.AssumeTrue(SecurityUtilTestHelper.IsExternalKdcRunning());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSecureNameNode()
		{
			MiniDFSCluster cluster = null;
			try
			{
				string nnPrincipal = Runtime.GetProperty("dfs.namenode.kerberos.principal");
				string nnSpnegoPrincipal = Runtime.GetProperty("dfs.namenode.kerberos.internal.spnego.principal"
					);
				string nnKeyTab = Runtime.GetProperty("dfs.namenode.keytab.file");
				NUnit.Framework.Assert.IsNotNull("NameNode principal was not specified", nnPrincipal
					);
				NUnit.Framework.Assert.IsNotNull("NameNode SPNEGO principal was not specified", nnSpnegoPrincipal
					);
				NUnit.Framework.Assert.IsNotNull("NameNode keytab was not specified", nnKeyTab);
				string dnPrincipal = Runtime.GetProperty("dfs.datanode.kerberos.principal");
				string dnKeyTab = Runtime.GetProperty("dfs.datanode.keytab.file");
				NUnit.Framework.Assert.IsNotNull("DataNode principal was not specified", dnPrincipal
					);
				NUnit.Framework.Assert.IsNotNull("DataNode keytab was not specified", dnKeyTab);
				Configuration conf = new HdfsConfiguration();
				conf.Set(CommonConfigurationKeys.HadoopSecurityAuthentication, "kerberos");
				conf.Set(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey, nnPrincipal);
				conf.Set(DFSConfigKeys.DfsNamenodeKerberosInternalSpnegoPrincipalKey, nnSpnegoPrincipal
					);
				conf.Set(DFSConfigKeys.DfsNamenodeKeytabFileKey, nnKeyTab);
				conf.Set(DFSConfigKeys.DfsDatanodeKerberosPrincipalKey, dnPrincipal);
				conf.Set(DFSConfigKeys.DfsDatanodeKeytabFileKey, dnKeyTab);
				// Secure DataNode requires using ports lower than 1024.
				conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, "127.0.0.1:1004");
				conf.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "127.0.0.1:1006");
				conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, "700");
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes).CheckDataNodeAddrConfig
					(true).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.IsTrue(cluster.IsDataNodeUp());
			}
			catch (Exception ex)
			{
				Sharpen.Runtime.PrintStackTrace(ex);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
