using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// Tests whether FileContext can resolve an hdfs path that has a symlink to
	/// local file system.
	/// </summary>
	/// <remarks>
	/// Tests whether FileContext can resolve an hdfs path that has a symlink to
	/// local file system. Also tests getDelegationTokens API in file context with
	/// underlying file system as Hdfs.
	/// </remarks>
	public class TestResolveHdfsSymlink
	{
		private static readonly FileContextTestHelper helper = new FileContextTestHelper(
			);

		private static MiniDFSCluster cluster = null;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUp()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
		}

		[AfterClass]
		public static void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Tests resolution of an hdfs symlink to the local file system.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFcResolveAfs()
		{
			Configuration conf = new Configuration();
			FileContext fcLocal = FileContext.GetLocalFSFileContext();
			FileContext fcHdfs = FileContext.GetFileContext(cluster.GetFileSystem().GetUri());
			string localTestRoot = helper.GetAbsoluteTestRootDir(fcLocal);
			Path alphaLocalPath = new Path(fcLocal.GetDefaultFileSystem().GetUri().ToString()
				, new FilePath(localTestRoot, "alpha").GetAbsolutePath());
			DFSTestUtil.CreateFile(FileSystem.GetLocal(conf), alphaLocalPath, 16, (short)1, 2
				);
			Path linkTarget = new Path(fcLocal.GetDefaultFileSystem().GetUri().ToString(), localTestRoot
				);
			Path hdfsLink = new Path(fcHdfs.GetDefaultFileSystem().GetUri().ToString(), "/tmp/link"
				);
			fcHdfs.CreateSymlink(linkTarget, hdfsLink, true);
			Path alphaHdfsPathViaLink = new Path(fcHdfs.GetDefaultFileSystem().GetUri().ToString
				() + "/tmp/link/alpha");
			ICollection<AbstractFileSystem> afsList = fcHdfs.ResolveAbstractFileSystems(alphaHdfsPathViaLink
				);
			NUnit.Framework.Assert.AreEqual(2, afsList.Count);
			foreach (AbstractFileSystem afs in afsList)
			{
				if ((!afs.Equals(fcHdfs.GetDefaultFileSystem())) && (!afs.Equals(fcLocal.GetDefaultFileSystem
					())))
				{
					NUnit.Framework.Assert.Fail("Failed to resolve AFS correctly");
				}
			}
		}

		/// <summary>
		/// Tests delegation token APIs in FileContext for Hdfs; and renew and cancel
		/// APIs in Hdfs.
		/// </summary>
		/// <exception cref="UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		[NUnit.Framework.Test]
		public virtual void TestFcDelegationToken()
		{
			FileContext fcHdfs = FileContext.GetFileContext(cluster.GetFileSystem().GetUri());
			AbstractFileSystem afs = fcHdfs.GetDefaultFileSystem();
			IList<Org.Apache.Hadoop.Security.Token.Token<object>> tokenList = afs.GetDelegationTokens
				(UserGroupInformation.GetCurrentUser().GetUserName());
			((Org.Apache.Hadoop.FS.Hdfs)afs).RenewDelegationToken((Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>)tokenList[0]);
			((Org.Apache.Hadoop.FS.Hdfs)afs).CancelDelegationToken((Org.Apache.Hadoop.Security.Token.Token
				<AbstractDelegationTokenIdentifier>)tokenList[0]);
		}

		/// <summary>
		/// Verifies that attempting to resolve a non-symlink results in client
		/// exception
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLinkTargetNonSymlink()
		{
			FileContext fc = null;
			Path notSymlink = new Path("/notasymlink");
			try
			{
				fc = FileContext.GetFileContext(cluster.GetFileSystem().GetUri());
				fc.Create(notSymlink, EnumSet.Of(CreateFlag.Create));
				DFSClient client = new DFSClient(cluster.GetFileSystem().GetUri(), cluster.GetConfiguration
					(0));
				try
				{
					client.GetLinkTarget(notSymlink.ToString());
					NUnit.Framework.Assert.Fail("Expected exception for resolving non-symlink");
				}
				catch (IOException e)
				{
					GenericTestUtils.AssertExceptionContains("is not a symbolic link", e);
				}
			}
			finally
			{
				if (fc != null)
				{
					fc.Delete(notSymlink, false);
				}
			}
		}

		/// <summary>Tests that attempting to resolve a non-existent-file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLinkTargetNonExistent()
		{
			Path doesNotExist = new Path("/filethatdoesnotexist");
			DFSClient client = new DFSClient(cluster.GetFileSystem().GetUri(), cluster.GetConfiguration
				(0));
			try
			{
				client.GetLinkTarget(doesNotExist.ToString());
				NUnit.Framework.Assert.Fail("Expected exception for resolving non-existent file");
			}
			catch (FileNotFoundException e)
			{
				GenericTestUtils.AssertExceptionContains("File does not exist: " + doesNotExist.ToString
					(), e);
			}
		}
	}
}
