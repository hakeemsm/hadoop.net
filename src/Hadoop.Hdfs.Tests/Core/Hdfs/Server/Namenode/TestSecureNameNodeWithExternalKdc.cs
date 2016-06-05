using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This test brings up a MiniDFSCluster with 1 NameNode and 0
	/// DataNodes with kerberos authentication enabled using user-specified
	/// KDC, principals, and keytabs.
	/// </summary>
	/// <remarks>
	/// This test brings up a MiniDFSCluster with 1 NameNode and 0
	/// DataNodes with kerberos authentication enabled using user-specified
	/// KDC, principals, and keytabs.
	/// To run, users must specify the following system properties:
	/// externalKdc=true
	/// java.security.krb5.conf
	/// dfs.namenode.kerberos.principal
	/// dfs.namenode.kerberos.internal.spnego.principal
	/// dfs.namenode.keytab.file
	/// user.principal (do not specify superuser!)
	/// user.keytab
	/// </remarks>
	public class TestSecureNameNodeWithExternalKdc
	{
		private const int NumOfDatanodes = 0;

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
				Configuration conf = new HdfsConfiguration();
				conf.Set(CommonConfigurationKeys.HadoopSecurityAuthentication, "kerberos");
				conf.Set(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey, nnPrincipal);
				conf.Set(DFSConfigKeys.DfsNamenodeKerberosInternalSpnegoPrincipalKey, nnSpnegoPrincipal
					);
				conf.Set(DFSConfigKeys.DfsNamenodeKeytabFileKey, nnKeyTab);
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(NumOfDatanodes).Build();
				MiniDFSCluster clusterRef = cluster;
				cluster.WaitActive();
				FileSystem fsForCurrentUser = cluster.GetFileSystem();
				fsForCurrentUser.Mkdirs(new Path("/tmp"));
				fsForCurrentUser.SetPermission(new Path("/tmp"), new FsPermission((short)511));
				// The user specified should not be a superuser
				string userPrincipal = Runtime.GetProperty("user.principal");
				string userKeyTab = Runtime.GetProperty("user.keytab");
				NUnit.Framework.Assert.IsNotNull("User principal was not specified", userPrincipal
					);
				NUnit.Framework.Assert.IsNotNull("User keytab was not specified", userKeyTab);
				UserGroupInformation ugi = UserGroupInformation.LoginUserFromKeytabAndReturnUGI(userPrincipal
					, userKeyTab);
				FileSystem fs = ugi.DoAs(new _PrivilegedExceptionAction_105(clusterRef));
				try
				{
					Path p = new Path("/users");
					fs.Mkdirs(p);
					NUnit.Framework.Assert.Fail("User must not be allowed to write in /");
				}
				catch (IOException)
				{
				}
				Path p_1 = new Path("/tmp/alpha");
				fs.Mkdirs(p_1);
				NUnit.Framework.Assert.IsNotNull(fs.ListStatus(p_1));
				NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Kerberos
					, ugi.GetAuthenticationMethod());
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_105 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_105(MiniDFSCluster clusterRef)
			{
				this.clusterRef = clusterRef;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return clusterRef.GetFileSystem();
			}

			private readonly MiniDFSCluster clusterRef;
		}
	}
}
