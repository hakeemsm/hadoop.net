using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestLease
	{
		internal static bool HasLease(MiniDFSCluster cluster, Path src)
		{
			return NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem()).GetLeaseByPath(src
				.ToString()) != null;
		}

		internal static int LeaseCount(MiniDFSCluster cluster)
		{
			return NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem()).CountLease();
		}

		internal const string dirString = "/test/lease";

		internal readonly Path dir = new Path(dirString);

		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestLease
			));

		internal readonly Configuration conf = new HdfsConfiguration();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseAbort()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				cluster.WaitActive();
				NamenodeProtocols preSpyNN = cluster.GetNameNodeRpc();
				NamenodeProtocols spyNN = Org.Mockito.Mockito.Spy(preSpyNN);
				DFSClient dfs = new DFSClient(null, spyNN, conf, null);
				byte[] buf = new byte[1024];
				FSDataOutputStream c_out = CreateFsOut(dfs, dirString + "c");
				c_out.Write(buf, 0, 1024);
				c_out.Close();
				DFSInputStream c_in = dfs.Open(dirString + "c");
				FSDataOutputStream d_out = CreateFsOut(dfs, dirString + "d");
				// stub the renew method.
				Org.Mockito.Mockito.DoThrow(new RemoteException(typeof(SecretManager.InvalidToken
					).FullName, "Your token is worthless")).When(spyNN).RenewLease(Matchers.AnyString
					());
				// We don't need to wait the lease renewer thread to act.
				// call renewLease() manually.
				// make it look like the soft limit has been exceeded.
				LeaseRenewer originalRenewer = dfs.GetLeaseRenewer();
				dfs.lastLeaseRenewal = Time.MonotonicNow() - HdfsConstants.LeaseSoftlimitPeriod -
					 1000;
				try
				{
					dfs.RenewLease();
				}
				catch (IOException)
				{
				}
				// Things should continue to work it passes hard limit without
				// renewing.
				try
				{
					d_out.Write(buf, 0, 1024);
					Log.Info("Write worked beyond the soft limit as expected.");
				}
				catch (IOException)
				{
					NUnit.Framework.Assert.Fail("Write failed.");
				}
				// make it look like the hard limit has been exceeded.
				dfs.lastLeaseRenewal = Time.MonotonicNow() - HdfsConstants.LeaseHardlimitPeriod -
					 1000;
				dfs.RenewLease();
				// this should not work.
				try
				{
					d_out.Write(buf, 0, 1024);
					d_out.Close();
					NUnit.Framework.Assert.Fail("Write did not fail even after the fatal lease renewal failure"
						);
				}
				catch (IOException e)
				{
					Log.Info("Write failed as expected. ", e);
				}
				// If aborted, the renewer should be empty. (no reference to clients)
				Sharpen.Thread.Sleep(1000);
				NUnit.Framework.Assert.IsTrue(originalRenewer.IsEmpty());
				// unstub
				Org.Mockito.Mockito.DoNothing().When(spyNN).RenewLease(Matchers.AnyString());
				// existing input streams should work
				try
				{
					int num = c_in.Read(buf, 0, 1);
					if (num != 1)
					{
						NUnit.Framework.Assert.Fail("Failed to read 1 byte");
					}
					c_in.Close();
				}
				catch (IOException e)
				{
					Log.Error("Read failed with ", e);
					NUnit.Framework.Assert.Fail("Read after lease renewal failure failed");
				}
				// new file writes should work.
				try
				{
					c_out = CreateFsOut(dfs, dirString + "c");
					c_out.Write(buf, 0, 1024);
					c_out.Close();
				}
				catch (IOException e)
				{
					Log.Error("Write failed with ", e);
					NUnit.Framework.Assert.Fail("Write failed");
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseAfterRename()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				Path p = new Path("/test-file");
				Path d = new Path("/test-d");
				Path d2 = new Path("/test-d-other");
				// open a file to get a lease
				FileSystem fs = cluster.GetFileSystem();
				FSDataOutputStream @out = fs.Create(p);
				@out.WriteBytes("something");
				//out.hsync();
				NUnit.Framework.Assert.IsTrue(HasLease(cluster, p));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				// just to ensure first fs doesn't have any logic to twiddle leases
				DistributedFileSystem fs2 = (DistributedFileSystem)FileSystem.NewInstance(fs.GetUri
					(), fs.GetConf());
				// rename the file into an existing dir
				Log.Info("DMS: rename file into dir");
				Path pRenamed = new Path(d, p.GetName());
				fs2.Mkdirs(d);
				fs2.Rename(p, pRenamed);
				NUnit.Framework.Assert.IsFalse(p + " exists", fs2.Exists(p));
				NUnit.Framework.Assert.IsTrue(pRenamed + " not found", fs2.Exists(pRenamed));
				NUnit.Framework.Assert.IsFalse("has lease for " + p, HasLease(cluster, p));
				NUnit.Framework.Assert.IsTrue("no lease for " + pRenamed, HasLease(cluster, pRenamed
					));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				// rename the parent dir to a new non-existent dir
				Log.Info("DMS: rename parent dir");
				Path pRenamedAgain = new Path(d2, pRenamed.GetName());
				fs2.Rename(d, d2);
				// src gone
				NUnit.Framework.Assert.IsFalse(d + " exists", fs2.Exists(d));
				NUnit.Framework.Assert.IsFalse("has lease for " + pRenamed, HasLease(cluster, pRenamed
					));
				// dst checks
				NUnit.Framework.Assert.IsTrue(d2 + " not found", fs2.Exists(d2));
				NUnit.Framework.Assert.IsTrue(pRenamedAgain + " not found", fs2.Exists(pRenamedAgain
					));
				NUnit.Framework.Assert.IsTrue("no lease for " + pRenamedAgain, HasLease(cluster, 
					pRenamedAgain));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				// rename the parent dir to existing dir
				// NOTE: rename w/o options moves paths into existing dir
				Log.Info("DMS: rename parent again");
				pRenamed = pRenamedAgain;
				pRenamedAgain = new Path(new Path(d, d2.GetName()), p.GetName());
				fs2.Mkdirs(d);
				fs2.Rename(d2, d);
				// src gone
				NUnit.Framework.Assert.IsFalse(d2 + " exists", fs2.Exists(d2));
				NUnit.Framework.Assert.IsFalse("no lease for " + pRenamed, HasLease(cluster, pRenamed
					));
				// dst checks
				NUnit.Framework.Assert.IsTrue(d + " not found", fs2.Exists(d));
				NUnit.Framework.Assert.IsTrue(pRenamedAgain + " not found", fs2.Exists(pRenamedAgain
					));
				NUnit.Framework.Assert.IsTrue("no lease for " + pRenamedAgain, HasLease(cluster, 
					pRenamedAgain));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				// rename with opts to non-existent dir
				pRenamed = pRenamedAgain;
				pRenamedAgain = new Path(d2, p.GetName());
				fs2.Rename(pRenamed.GetParent(), d2, Options.Rename.Overwrite);
				// src gone
				NUnit.Framework.Assert.IsFalse(pRenamed.GetParent() + " not found", fs2.Exists(pRenamed
					.GetParent()));
				NUnit.Framework.Assert.IsFalse("has lease for " + pRenamed, HasLease(cluster, pRenamed
					));
				// dst checks
				NUnit.Framework.Assert.IsTrue(d2 + " not found", fs2.Exists(d2));
				NUnit.Framework.Assert.IsTrue(pRenamedAgain + " not found", fs2.Exists(pRenamedAgain
					));
				NUnit.Framework.Assert.IsTrue("no lease for " + pRenamedAgain, HasLease(cluster, 
					pRenamedAgain));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				// rename with opts to existing dir
				// NOTE: rename with options will not move paths into the existing dir
				pRenamed = pRenamedAgain;
				pRenamedAgain = new Path(d, p.GetName());
				fs2.Rename(pRenamed.GetParent(), d, Options.Rename.Overwrite);
				// src gone
				NUnit.Framework.Assert.IsFalse(pRenamed.GetParent() + " not found", fs2.Exists(pRenamed
					.GetParent()));
				NUnit.Framework.Assert.IsFalse("has lease for " + pRenamed, HasLease(cluster, pRenamed
					));
				// dst checks
				NUnit.Framework.Assert.IsTrue(d + " not found", fs2.Exists(d));
				NUnit.Framework.Assert.IsTrue(pRenamedAgain + " not found", fs2.Exists(pRenamedAgain
					));
				NUnit.Framework.Assert.IsTrue("no lease for " + pRenamedAgain, HasLease(cluster, 
					pRenamedAgain));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				@out.Close();
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test that we can open up a file for write, move it to another location,
		/// and then create a new file in the previous location, without causing any
		/// lease conflicts.
		/// </summary>
		/// <remarks>
		/// Test that we can open up a file for write, move it to another location,
		/// and then create a new file in the previous location, without causing any
		/// lease conflicts.  This is possible because we now use unique inode IDs
		/// to identify files to the NameNode.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseAfterRenameAndRecreate()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				Path path1 = new Path("/test-file");
				string contents1 = "contents1";
				Path path2 = new Path("/test-file-new-location");
				string contents2 = "contents2";
				// open a file to get a lease
				FileSystem fs = cluster.GetFileSystem();
				FSDataOutputStream out1 = fs.Create(path1);
				out1.WriteBytes(contents1);
				NUnit.Framework.Assert.IsTrue(HasLease(cluster, path1));
				NUnit.Framework.Assert.AreEqual(1, LeaseCount(cluster));
				DistributedFileSystem fs2 = (DistributedFileSystem)FileSystem.NewInstance(fs.GetUri
					(), fs.GetConf());
				fs2.Rename(path1, path2);
				FSDataOutputStream out2 = fs2.Create(path1);
				out2.WriteBytes(contents2);
				out2.Close();
				// The first file should still be open and valid
				NUnit.Framework.Assert.IsTrue(HasLease(cluster, path2));
				out1.Close();
				// Contents should be as expected
				DistributedFileSystem fs3 = (DistributedFileSystem)FileSystem.NewInstance(fs.GetUri
					(), fs.GetConf());
				NUnit.Framework.Assert.AreEqual(contents1, DFSTestUtil.ReadFile(fs3, path2));
				NUnit.Framework.Assert.AreEqual(contents2, DFSTestUtil.ReadFile(fs3, path1));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLease()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				Path a = new Path(dir, "a");
				Path b = new Path(dir, "b");
				DataOutputStream a_out = fs.Create(a);
				a_out.WriteBytes("something");
				NUnit.Framework.Assert.IsTrue(HasLease(cluster, a));
				NUnit.Framework.Assert.IsTrue(!HasLease(cluster, b));
				DataOutputStream b_out = fs.Create(b);
				b_out.WriteBytes("something");
				NUnit.Framework.Assert.IsTrue(HasLease(cluster, a));
				NUnit.Framework.Assert.IsTrue(HasLease(cluster, b));
				a_out.Close();
				b_out.Close();
				NUnit.Framework.Assert.IsTrue(!HasLease(cluster, a));
				NUnit.Framework.Assert.IsTrue(!HasLease(cluster, b));
				fs.Delete(dir, true);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFactory()
		{
			string[] groups = new string[] { "supergroup" };
			UserGroupInformation[] ugi = new UserGroupInformation[3];
			for (int i = 0; i < ugi.Length; i++)
			{
				ugi[i] = UserGroupInformation.CreateUserForTesting("user" + i, groups);
			}
			Org.Mockito.Mockito.DoReturn(new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission
				((short)777), "owner", "group", new byte[0], new byte[0], 1010, 0, null, unchecked(
				(byte)0))).When(mcp).GetFileInfo(Matchers.AnyString());
			Org.Mockito.Mockito.DoReturn(new HdfsFileStatus(0, false, 1, 1024, 0, 0, new FsPermission
				((short)777), "owner", "group", new byte[0], new byte[0], 1010, 0, null, unchecked(
				(byte)0))).When(mcp).Create(Matchers.AnyString(), (FsPermission)Matchers.AnyObject
				(), Matchers.AnyString(), (EnumSetWritable<CreateFlag>)Matchers.AnyObject(), Matchers.AnyBoolean
				(), Matchers.AnyShort(), Matchers.AnyLong(), (CryptoProtocolVersion[])Matchers.AnyObject
				());
			Configuration conf = new Configuration();
			DFSClient c1 = CreateDFSClientAs(ugi[0], conf);
			FSDataOutputStream out1 = CreateFsOut(c1, "/out1");
			DFSClient c2 = CreateDFSClientAs(ugi[0], conf);
			FSDataOutputStream out2 = CreateFsOut(c2, "/out2");
			NUnit.Framework.Assert.AreEqual(c1.GetLeaseRenewer(), c2.GetLeaseRenewer());
			DFSClient c3 = CreateDFSClientAs(ugi[1], conf);
			FSDataOutputStream out3 = CreateFsOut(c3, "/out3");
			NUnit.Framework.Assert.IsTrue(c1.GetLeaseRenewer() != c3.GetLeaseRenewer());
			DFSClient c4 = CreateDFSClientAs(ugi[1], conf);
			FSDataOutputStream out4 = CreateFsOut(c4, "/out4");
			NUnit.Framework.Assert.AreEqual(c3.GetLeaseRenewer(), c4.GetLeaseRenewer());
			DFSClient c5 = CreateDFSClientAs(ugi[2], conf);
			FSDataOutputStream out5 = CreateFsOut(c5, "/out5");
			NUnit.Framework.Assert.IsTrue(c1.GetLeaseRenewer() != c5.GetLeaseRenewer());
			NUnit.Framework.Assert.IsTrue(c3.GetLeaseRenewer() != c5.GetLeaseRenewer());
		}

		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream CreateFsOut(DFSClient dfs, string path)
		{
			return new FSDataOutputStream(dfs.Create(path, true), null);
		}

		internal static readonly ClientProtocol mcp = Org.Mockito.Mockito.Mock<ClientProtocol
			>();

		/// <exception cref="System.Exception"/>
		public static DFSClient CreateDFSClientAs(UserGroupInformation ugi, Configuration
			 conf)
		{
			return ugi.DoAs(new _PrivilegedExceptionAction_384(conf));
		}

		private sealed class _PrivilegedExceptionAction_384 : PrivilegedExceptionAction<DFSClient
			>
		{
			public _PrivilegedExceptionAction_384(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public DFSClient Run()
			{
				return new DFSClient(null, Org.Apache.Hadoop.Hdfs.TestLease.mcp, conf, null);
			}

			private readonly Configuration conf;
		}
	}
}
