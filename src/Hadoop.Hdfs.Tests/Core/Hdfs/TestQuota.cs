using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>A class for testing quota-related commands</summary>
	public class TestQuota
	{
		/// <exception cref="System.Exception"/>
		private void RunCommand(DFSAdmin admin, bool expectError, params string[] args)
		{
			RunCommand(admin, args, expectError);
		}

		/// <exception cref="System.Exception"/>
		private void RunCommand(DFSAdmin admin, string[] args, bool expectEror)
		{
			int val = admin.Run(args);
			if (expectEror)
			{
				NUnit.Framework.Assert.AreEqual(val, -1);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(val >= 0);
			}
		}

		/// <summary>
		/// Tests to make sure we're getting human readable Quota exception messages
		/// Test for @link{ NSQuotaExceededException, DSQuotaExceededException}
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDSQuotaExceededExceptionIsHumanReadable()
		{
			int bytes = 1024;
			try
			{
				throw new DSQuotaExceededException(bytes, bytes);
			}
			catch (DSQuotaExceededException e)
			{
				NUnit.Framework.Assert.AreEqual("The DiskSpace quota is exceeded: quota = 1024 B = 1 KB"
					 + " but diskspace consumed = 1024 B = 1 KB", e.Message);
			}
		}

		/// <summary>
		/// Test quota related commands:
		/// setQuota, clrQuota, setSpaceQuota, clrSpaceQuota, and count
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQuotaCommands()
		{
			Configuration conf = new HdfsConfiguration();
			// set a smaller block size so that we can test with smaller 
			// Space quotas
			int DefaultBlockSize = 512;
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, DefaultBlockSize);
			// Make it relinquish locks. When run serially, the result should
			// be identical.
			conf.SetInt(DFSConfigKeys.DfsContentSummaryLimitKey, 2);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			DFSAdmin admin = new DFSAdmin(conf);
			try
			{
				int fileLen = 1024;
				short replication = 5;
				long spaceQuota = fileLen * replication * 15 / 8;
				// 1: create a directory /test and set its quota to be 3
				Path parent = new Path("/test");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(parent));
				string[] args = new string[] { "-setQuota", "3", parent.ToString() };
				RunCommand(admin, args, false);
				//try setting space quota with a 'binary prefix'
				RunCommand(admin, false, "-setSpaceQuota", "2t", parent.ToString());
				NUnit.Framework.Assert.AreEqual(2L << 40, dfs.GetContentSummary(parent).GetSpaceQuota
					());
				// set diskspace quota to 10000 
				RunCommand(admin, false, "-setSpaceQuota", System.Convert.ToString(spaceQuota), parent
					.ToString());
				// 2: create directory /test/data0
				Path childDir0 = new Path(parent, "data0");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(childDir0));
				// 3: create a file /test/datafile0
				Path childFile0 = new Path(parent, "datafile0");
				DFSTestUtil.CreateFile(fs, childFile0, fileLen, replication, 0);
				// 4: count -q /test
				ContentSummary c = dfs.GetContentSummary(parent);
				NUnit.Framework.Assert.AreEqual(c.GetFileCount() + c.GetDirectoryCount(), 3);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 3);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), fileLen * replication);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), spaceQuota);
				// 5: count -q /test/data0
				c = dfs.GetContentSummary(childDir0);
				NUnit.Framework.Assert.AreEqual(c.GetFileCount() + c.GetDirectoryCount(), 1);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), -1);
				// check disk space consumed
				c = dfs.GetContentSummary(parent);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), fileLen * replication);
				// 6: create a directory /test/data1
				Path childDir1 = new Path(parent, "data1");
				bool hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Mkdirs(childDir1));
				}
				catch (QuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				OutputStream fout;
				// 7: create a file /test/datafile1
				Path childFile1 = new Path(parent, "datafile1");
				hasException = false;
				try
				{
					fout = dfs.Create(childFile1);
				}
				catch (QuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// 8: clear quota /test
				RunCommand(admin, new string[] { "-clrQuota", parent.ToString() }, false);
				c = dfs.GetContentSummary(parent);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), -1);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), spaceQuota);
				// 9: clear quota /test/data0
				RunCommand(admin, new string[] { "-clrQuota", childDir0.ToString() }, false);
				c = dfs.GetContentSummary(childDir0);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), -1);
				// 10: create a file /test/datafile1
				fout = dfs.Create(childFile1, replication);
				// 10.s: but writing fileLen bytes should result in an quota exception
				try
				{
					fout.Write(new byte[fileLen]);
					fout.Close();
					NUnit.Framework.Assert.Fail();
				}
				catch (QuotaExceededException)
				{
					IOUtils.CloseStream(fout);
				}
				//delete the file
				dfs.Delete(childFile1, false);
				// 9.s: clear diskspace quota
				RunCommand(admin, false, "-clrSpaceQuota", parent.ToString());
				c = dfs.GetContentSummary(parent);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), -1);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), -1);
				// now creating childFile1 should succeed
				DFSTestUtil.CreateFile(dfs, childFile1, fileLen, replication, 0);
				// 11: set the quota of /test to be 1
				// HADOOP-5872 - we can set quota even if it is immediately violated 
				args = new string[] { "-setQuota", "1", parent.ToString() };
				RunCommand(admin, args, false);
				RunCommand(admin, false, "-setSpaceQuota", Sharpen.Extensions.ToString(fileLen), 
					args[2]);
				// for space quota
				// 12: set the quota of /test/data0 to be 1
				args = new string[] { "-setQuota", "1", childDir0.ToString() };
				RunCommand(admin, args, false);
				// 13: not able create a directory under data0
				hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Mkdirs(new Path(childDir0, "in")));
				}
				catch (QuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				c = dfs.GetContentSummary(childDir0);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount() + c.GetFileCount(), 1);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 1);
				// 14a: set quota on a non-existent directory
				Path nonExistentPath = new Path("/test1");
				NUnit.Framework.Assert.IsFalse(dfs.Exists(nonExistentPath));
				args = new string[] { "-setQuota", "1", nonExistentPath.ToString() };
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-setSpaceQuota", "1g", nonExistentPath.ToString());
				// for space quota
				// 14b: set quota on a file
				NUnit.Framework.Assert.IsTrue(dfs.IsFile(childFile0));
				args[1] = childFile0.ToString();
				RunCommand(admin, args, true);
				// same for space quota
				RunCommand(admin, true, "-setSpaceQuota", "1t", args[1]);
				// 15a: clear quota on a file
				args[0] = "-clrQuota";
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-clrSpaceQuota", args[1]);
				// 15b: clear quota on a non-existent directory
				args[1] = nonExistentPath.ToString();
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-clrSpaceQuota", args[1]);
				// 16a: set the quota of /test to be 0
				args = new string[] { "-setQuota", "0", parent.ToString() };
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-setSpaceQuota", "0", args[2]);
				// 16b: set the quota of /test to be -1
				args[1] = "-1";
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
				// 16c: set the quota of /test to be Long.MAX_VALUE+1
				args[1] = (long.MaxValue + 1L).ToString();
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
				// 16d: set the quota of /test to be a non integer
				args[1] = "33aa1.5";
				RunCommand(admin, args, true);
				RunCommand(admin, true, "-setSpaceQuota", args[1], args[2]);
				// 16e: set space quota with a value larger than Long.MAX_VALUE
				RunCommand(admin, true, "-setSpaceQuota", (long.MaxValue / 1024 / 1024 + 1024) + 
					"m", args[2]);
				// 17:  setQuota by a non-administrator
				string username = "userxx";
				UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(username, new 
					string[] { "groupyy" });
				string[] args2 = args.MemberwiseClone();
				// need final ref for doAs block
				ugi.DoAs(new _PrivilegedExceptionAction_275(this, username, conf, args2, parent));
				// 18: clrQuota by a non-administrator
				// 19: clrQuota on the root directory ("/") should fail
				RunCommand(admin, true, "-clrQuota", "/");
				// 20: setQuota on the root directory ("/") should succeed
				RunCommand(admin, false, "-setQuota", "1000000", "/");
				RunCommand(admin, true, "-clrQuota", "/");
				RunCommand(admin, false, "-clrSpaceQuota", "/");
				RunCommand(admin, new string[] { "-clrQuota", parent.ToString() }, false);
				RunCommand(admin, false, "-clrSpaceQuota", parent.ToString());
				// 2: create directory /test/data2
				Path childDir2 = new Path(parent, "data2");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(childDir2));
				Path childFile2 = new Path(childDir2, "datafile2");
				Path childFile3 = new Path(childDir2, "datafile3");
				long spaceQuota2 = DefaultBlockSize * replication;
				long fileLen2 = DefaultBlockSize;
				// set space quota to a real low value 
				RunCommand(admin, false, "-setSpaceQuota", System.Convert.ToString(spaceQuota2), 
					childDir2.ToString());
				// clear space quota
				RunCommand(admin, false, "-clrSpaceQuota", childDir2.ToString());
				// create a file that is greater than the size of space quota
				DFSTestUtil.CreateFile(fs, childFile2, fileLen2, replication, 0);
				// now set space quota again. This should succeed
				RunCommand(admin, false, "-setSpaceQuota", System.Convert.ToString(spaceQuota2), 
					childDir2.ToString());
				hasException = false;
				try
				{
					DFSTestUtil.CreateFile(fs, childFile3, fileLen2, replication, 0);
				}
				catch (DSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// now test the same for root
				Path childFile4 = new Path("/", "datafile2");
				Path childFile5 = new Path("/", "datafile3");
				RunCommand(admin, true, "-clrQuota", "/");
				RunCommand(admin, false, "-clrSpaceQuota", "/");
				// set space quota to a real low value 
				RunCommand(admin, false, "-setSpaceQuota", System.Convert.ToString(spaceQuota2), 
					"/");
				RunCommand(admin, false, "-clrSpaceQuota", "/");
				DFSTestUtil.CreateFile(fs, childFile4, fileLen2, replication, 0);
				RunCommand(admin, false, "-setSpaceQuota", System.Convert.ToString(spaceQuota2), 
					"/");
				hasException = false;
				try
				{
					DFSTestUtil.CreateFile(fs, childFile5, fileLen2, replication, 0);
				}
				catch (DSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				NUnit.Framework.Assert.AreEqual(4, cluster.GetNamesystem().GetFSDirectory().GetYieldCount
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _PrivilegedExceptionAction_275 : PrivilegedExceptionAction<object
			>
		{
			public _PrivilegedExceptionAction_275(TestQuota _enclosing, string username, Configuration
				 conf, string[] args2, Path parent)
			{
				this._enclosing = _enclosing;
				this.username = username;
				this.conf = conf;
				this.args2 = args2;
				this.parent = parent;
			}

			/// <exception cref="System.Exception"/>
			public object Run()
			{
				NUnit.Framework.Assert.AreEqual("Not running as new user", username, UserGroupInformation
					.GetCurrentUser().GetShortUserName());
				DFSAdmin userAdmin = new DFSAdmin(conf);
				args2[1] = "100";
				this._enclosing.RunCommand(userAdmin, args2, true);
				this._enclosing.RunCommand(userAdmin, true, "-setSpaceQuota", "1g", args2[2]);
				string[] args3 = new string[] { "-clrQuota", parent.ToString() };
				this._enclosing.RunCommand(userAdmin, args3, true);
				this._enclosing.RunCommand(userAdmin, true, "-clrSpaceQuota", args3[1]);
				return null;
			}

			private readonly TestQuota _enclosing;

			private readonly string username;

			private readonly Configuration conf;

			private readonly string[] args2;

			private readonly Path parent;
		}

		/// <summary>
		/// Test commands that change the size of the name space:
		/// mkdirs, rename, and delete
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNamespaceCommands()
		{
			Configuration conf = new HdfsConfiguration();
			// Make it relinquish locks. When run serially, the result should
			// be identical.
			conf.SetInt(DFSConfigKeys.DfsContentSummaryLimitKey, 2);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			try
			{
				// 1: create directory /nqdir0/qdir1/qdir20/nqdir30
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")
					));
				// 2: set the quota of /nqdir0/qdir1 to be 6
				Path quotaDir1 = new Path("/nqdir0/qdir1");
				dfs.SetQuota(quotaDir1, 6, HdfsConstants.QuotaDontSet);
				ContentSummary c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 3);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				// 3: set the quota of /nqdir0/qdir1/qdir20 to be 7
				Path quotaDir2 = new Path("/nqdir0/qdir1/qdir20");
				dfs.SetQuota(quotaDir2, 7, HdfsConstants.QuotaDontSet);
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 2);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				// 4: Create directory /nqdir0/qdir1/qdir21 and set its quota to 2
				Path quotaDir3 = new Path("/nqdir0/qdir1/qdir21");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(quotaDir3));
				dfs.SetQuota(quotaDir3, 2, HdfsConstants.QuotaDontSet);
				c = dfs.GetContentSummary(quotaDir3);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 1);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 2);
				// 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
				Path tempPath = new Path(quotaDir3, "nqdir32");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(tempPath));
				c = dfs.GetContentSummary(quotaDir3);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 2);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 2);
				// 6: Create directory /nqdir0/qdir1/qdir21/nqdir33
				tempPath = new Path(quotaDir3, "nqdir33");
				bool hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Mkdirs(tempPath));
				}
				catch (NSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				c = dfs.GetContentSummary(quotaDir3);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 2);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 2);
				// 7: Create directory /nqdir0/qdir1/qdir20/nqdir31
				tempPath = new Path(quotaDir2, "nqdir31");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(tempPath));
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 3);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 6);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				// 8: Create directory /nqdir0/qdir1/qdir20/nqdir33
				tempPath = new Path(quotaDir2, "nqdir33");
				hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Mkdirs(tempPath));
				}
				catch (NSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// 9: Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
				tempPath = new Path(quotaDir2, "nqdir30");
				dfs.Rename(new Path(quotaDir3, "nqdir32"), tempPath);
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 4);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 6);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				// 10: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21
				hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Rename(tempPath, quotaDir3));
				}
				catch (NSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				NUnit.Framework.Assert.IsTrue(dfs.Exists(tempPath));
				NUnit.Framework.Assert.IsFalse(dfs.Exists(new Path(quotaDir3, "nqdir30")));
				// 10.a: Rename /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/nqdir32
				hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Rename(tempPath, new Path(quotaDir3, "nqdir32"
						)));
				}
				catch (QuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				NUnit.Framework.Assert.IsTrue(dfs.Exists(tempPath));
				NUnit.Framework.Assert.IsFalse(dfs.Exists(new Path(quotaDir3, "nqdir32")));
				// 11: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0
				NUnit.Framework.Assert.IsTrue(dfs.Rename(tempPath, new Path("/nqdir0")));
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 2);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 4);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				// 12: Create directory /nqdir0/nqdir30/nqdir33
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(new Path("/nqdir0/nqdir30/nqdir33")));
				// 13: Move /nqdir0/nqdir30 /nqdir0/qdir1/qdir20/qdir30
				hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Rename(new Path("/nqdir0/nqdir30"), tempPath));
				}
				catch (NSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// 14: Move /nqdir0/qdir1/qdir21 /nqdir0/qdir1/qdir20
				NUnit.Framework.Assert.IsTrue(dfs.Rename(quotaDir3, quotaDir2));
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 4);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 3);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				tempPath = new Path(quotaDir2, "qdir21");
				c = dfs.GetContentSummary(tempPath);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 1);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 2);
				// 15: Delete /nqdir0/qdir1/qdir20/qdir21
				dfs.Delete(tempPath, true);
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 2);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 3);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				// 16: Move /nqdir0/qdir30 /nqdir0/qdir1/qdir20
				NUnit.Framework.Assert.IsTrue(dfs.Rename(new Path("/nqdir0/nqdir30"), quotaDir2));
				c = dfs.GetContentSummary(quotaDir2);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 5);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 7);
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetDirectoryCount(), 6);
				NUnit.Framework.Assert.AreEqual(c.GetQuota(), 6);
				NUnit.Framework.Assert.AreEqual(14, cluster.GetNamesystem().GetFSDirectory().GetYieldCount
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Test HDFS operations that change disk space consumed by a directory tree.
		/// 	</summary>
		/// <remarks>
		/// Test HDFS operations that change disk space consumed by a directory tree.
		/// namely create, rename, delete, append, and setReplication.
		/// This is based on testNamespaceCommands() above.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSpaceCommands()
		{
			Configuration conf = new HdfsConfiguration();
			// set a smaller block size so that we can test with smaller 
			// diskspace quotas
			conf.Set(DFSConfigKeys.DfsBlockSizeKey, "512");
			// Make it relinquish locks. When run serially, the result should
			// be identical.
			conf.SetInt(DFSConfigKeys.DfsContentSummaryLimitKey, 2);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			FileSystem fs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("Not a HDFS: " + fs.GetUri(), fs is DistributedFileSystem
				);
			DistributedFileSystem dfs = (DistributedFileSystem)fs;
			try
			{
				int fileLen = 1024;
				short replication = 3;
				int fileSpace = fileLen * replication;
				// create directory /nqdir0/qdir1/qdir20/nqdir30
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(new Path("/nqdir0/qdir1/qdir20/nqdir30")
					));
				// set the quota of /nqdir0/qdir1 to 4 * fileSpace 
				Path quotaDir1 = new Path("/nqdir0/qdir1");
				dfs.SetQuota(quotaDir1, HdfsConstants.QuotaDontSet, 4 * fileSpace);
				ContentSummary c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), 4 * fileSpace);
				// set the quota of /nqdir0/qdir1/qdir20 to 6 * fileSpace 
				Path quotaDir20 = new Path("/nqdir0/qdir1/qdir20");
				dfs.SetQuota(quotaDir20, HdfsConstants.QuotaDontSet, 6 * fileSpace);
				c = dfs.GetContentSummary(quotaDir20);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), 6 * fileSpace);
				// Create /nqdir0/qdir1/qdir21 and set its space quota to 2 * fileSpace
				Path quotaDir21 = new Path("/nqdir0/qdir1/qdir21");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(quotaDir21));
				dfs.SetQuota(quotaDir21, HdfsConstants.QuotaDontSet, 2 * fileSpace);
				c = dfs.GetContentSummary(quotaDir21);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), 2 * fileSpace);
				// 5: Create directory /nqdir0/qdir1/qdir21/nqdir32
				Path tempPath = new Path(quotaDir21, "nqdir32");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(tempPath));
				// create a file under nqdir32/fileDir
				DFSTestUtil.CreateFile(dfs, new Path(tempPath, "fileDir/file1"), fileLen, replication
					, 0);
				c = dfs.GetContentSummary(quotaDir21);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), fileSpace);
				// Create a larger file /nqdir0/qdir1/qdir21/nqdir33/
				bool hasException = false;
				try
				{
					DFSTestUtil.CreateFile(dfs, new Path(quotaDir21, "nqdir33/file2"), 2 * fileLen, replication
						, 0);
				}
				catch (DSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// delete nqdir33
				NUnit.Framework.Assert.IsTrue(dfs.Delete(new Path(quotaDir21, "nqdir33"), true));
				c = dfs.GetContentSummary(quotaDir21);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), fileSpace);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), 2 * fileSpace);
				// Verify space before the move:
				c = dfs.GetContentSummary(quotaDir20);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 0);
				// Move /nqdir0/qdir1/qdir21/nqdir32 /nqdir0/qdir1/qdir20/nqdir30
				Path dstPath = new Path(quotaDir20, "nqdir30");
				Path srcPath = new Path(quotaDir21, "nqdir32");
				NUnit.Framework.Assert.IsTrue(dfs.Rename(srcPath, dstPath));
				// verify space after the move
				c = dfs.GetContentSummary(quotaDir20);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), fileSpace);
				// verify space for its parent
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), fileSpace);
				// verify space for source for the move
				c = dfs.GetContentSummary(quotaDir21);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 0);
				Path file2 = new Path(dstPath, "fileDir/file2");
				int file2Len = 2 * fileLen;
				// create a larger file under /nqdir0/qdir1/qdir20/nqdir30
				DFSTestUtil.CreateFile(dfs, file2, file2Len, replication, 0);
				c = dfs.GetContentSummary(quotaDir20);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 3 * fileSpace);
				c = dfs.GetContentSummary(quotaDir21);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 0);
				// Reverse: Move /nqdir0/qdir1/qdir20/nqdir30 to /nqdir0/qdir1/qdir21/
				hasException = false;
				try
				{
					NUnit.Framework.Assert.IsFalse(dfs.Rename(dstPath, srcPath));
				}
				catch (DSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// make sure no intermediate directories left by failed rename
				NUnit.Framework.Assert.IsFalse(dfs.Exists(srcPath));
				// directory should exist
				NUnit.Framework.Assert.IsTrue(dfs.Exists(dstPath));
				// verify space after the failed move
				c = dfs.GetContentSummary(quotaDir20);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 3 * fileSpace);
				c = dfs.GetContentSummary(quotaDir21);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 0);
				// Test Append :
				// verify space quota
				c = dfs.GetContentSummary(quotaDir1);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), 4 * fileSpace);
				// verify space before append;
				c = dfs.GetContentSummary(dstPath);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 3 * fileSpace);
				OutputStream @out = dfs.Append(file2);
				// appending 1 fileLen should succeed
				@out.Write(new byte[fileLen]);
				@out.Close();
				file2Len += fileLen;
				// after append
				// verify space after append;
				c = dfs.GetContentSummary(dstPath);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 4 * fileSpace);
				// now increase the quota for quotaDir1
				dfs.SetQuota(quotaDir1, HdfsConstants.QuotaDontSet, 5 * fileSpace);
				// Now, appending more than 1 fileLen should result in an error
				@out = dfs.Append(file2);
				hasException = false;
				try
				{
					@out.Write(new byte[fileLen + 1024]);
					@out.Flush();
					@out.Close();
				}
				catch (DSQuotaExceededException)
				{
					hasException = true;
					IOUtils.CloseStream(@out);
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				file2Len += fileLen;
				// after partial append
				// verify space after partial append
				c = dfs.GetContentSummary(dstPath);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 5 * fileSpace);
				// Test set replication :
				// first reduce the replication
				dfs.SetReplication(file2, (short)(replication - 1));
				// verify that space is reduced by file2Len
				c = dfs.GetContentSummary(dstPath);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 5 * fileSpace - file2Len);
				// now try to increase the replication and and expect an error.
				hasException = false;
				try
				{
					dfs.SetReplication(file2, (short)(replication + 1));
				}
				catch (DSQuotaExceededException)
				{
					hasException = true;
				}
				NUnit.Framework.Assert.IsTrue(hasException);
				// verify space consumed remains unchanged.
				c = dfs.GetContentSummary(dstPath);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 5 * fileSpace - file2Len);
				// now increase the quota for quotaDir1 and quotaDir20
				dfs.SetQuota(quotaDir1, HdfsConstants.QuotaDontSet, 10 * fileSpace);
				dfs.SetQuota(quotaDir20, HdfsConstants.QuotaDontSet, 10 * fileSpace);
				// then increasing replication should be ok.
				dfs.SetReplication(file2, (short)(replication + 1));
				// verify increase in space
				c = dfs.GetContentSummary(dstPath);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), 5 * fileSpace + file2Len);
				// Test HDFS-2053 :
				// Create directory /hdfs-2053
				Path quotaDir2053 = new Path("/hdfs-2053");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(quotaDir2053));
				// Create subdirectories /hdfs-2053/{A,B,C}
				Path quotaDir2053_A = new Path(quotaDir2053, "A");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(quotaDir2053_A));
				Path quotaDir2053_B = new Path(quotaDir2053, "B");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(quotaDir2053_B));
				Path quotaDir2053_C = new Path(quotaDir2053, "C");
				NUnit.Framework.Assert.IsTrue(dfs.Mkdirs(quotaDir2053_C));
				// Factors to vary the sizes of test files created in each subdir.
				// The actual factors are not really important but they allow us to create
				// identifiable file sizes per subdir, which helps during debugging.
				int sizeFactorA = 1;
				int sizeFactorB = 2;
				int sizeFactorC = 4;
				// Set space quota for subdirectory C
				dfs.SetQuota(quotaDir2053_C, HdfsConstants.QuotaDontSet, (sizeFactorC + 1) * fileSpace
					);
				c = dfs.GetContentSummary(quotaDir2053_C);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceQuota(), (sizeFactorC + 1) * fileSpace);
				// Create a file under subdirectory A
				DFSTestUtil.CreateFile(dfs, new Path(quotaDir2053_A, "fileA"), sizeFactorA * fileLen
					, replication, 0);
				c = dfs.GetContentSummary(quotaDir2053_A);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), sizeFactorA * fileSpace);
				// Create a file under subdirectory B
				DFSTestUtil.CreateFile(dfs, new Path(quotaDir2053_B, "fileB"), sizeFactorB * fileLen
					, replication, 0);
				c = dfs.GetContentSummary(quotaDir2053_B);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), sizeFactorB * fileSpace);
				// Create a file under subdirectory C (which has a space quota)
				DFSTestUtil.CreateFile(dfs, new Path(quotaDir2053_C, "fileC"), sizeFactorC * fileLen
					, replication, 0);
				c = dfs.GetContentSummary(quotaDir2053_C);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), sizeFactorC * fileSpace);
				// Check space consumed for /hdfs-2053
				c = dfs.GetContentSummary(quotaDir2053);
				NUnit.Framework.Assert.AreEqual(c.GetSpaceConsumed(), (sizeFactorA + sizeFactorB 
					+ sizeFactorC) * fileSpace);
				NUnit.Framework.Assert.AreEqual(20, cluster.GetNamesystem().GetFSDirectory().GetYieldCount
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static void CheckContentSummary(ContentSummary expected, ContentSummary computed
			)
		{
			NUnit.Framework.Assert.AreEqual(expected.ToString(), computed.ToString());
		}

		/// <summary>Violate a space quota using files of size &lt; 1 block.</summary>
		/// <remarks>
		/// Violate a space quota using files of size &lt; 1 block. Test that block
		/// allocation conservatively assumes that for quota checking the entire
		/// space of the block is used.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockAllocationAdjustsUsageConservatively()
		{
			Configuration conf = new HdfsConfiguration();
			int BlockSize = 6 * 1024;
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			DFSAdmin admin = new DFSAdmin(conf);
			string nnAddr = conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey);
			string webhdfsuri = WebHdfsFileSystem.Scheme + "://" + nnAddr;
			System.Console.Out.WriteLine("webhdfsuri=" + webhdfsuri);
			FileSystem webhdfs = new Path(webhdfsuri).GetFileSystem(conf);
			try
			{
				Path dir = new Path("/test");
				Path file1 = new Path("/test/test1");
				Path file2 = new Path("/test/test2");
				bool exceededQuota = false;
				int QuotaSize = 3 * BlockSize;
				// total space usage including
				// repl.
				int FileSize = BlockSize / 2;
				ContentSummary c;
				// Create the directory and set the quota
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				RunCommand(admin, false, "-setSpaceQuota", Sharpen.Extensions.ToString(QuotaSize)
					, dir.ToString());
				// Creating a file should use half the quota
				DFSTestUtil.CreateFile(fs, file1, FileSize, (short)3, 1L);
				DFSTestUtil.WaitReplication(fs, file1, (short)3);
				c = fs.GetContentSummary(dir);
				CheckContentSummary(c, webhdfs.GetContentSummary(dir));
				NUnit.Framework.Assert.AreEqual("Quota is half consumed", QuotaSize / 2, c.GetSpaceConsumed
					());
				// We can not create the 2nd file because even though the total spaced
				// used by two files (2 * 3 * 512/2) would fit within the quota (3 * 512)
				// when a block for a file is created the space used is adjusted
				// conservatively (3 * block size, ie assumes a full block is written)
				// which will violate the quota (3 * block size) since we've already 
				// used half the quota for the first file.
				try
				{
					DFSTestUtil.CreateFile(fs, file2, FileSize, (short)3, 1L);
				}
				catch (QuotaExceededException)
				{
					exceededQuota = true;
				}
				NUnit.Framework.Assert.IsTrue("Quota not exceeded", exceededQuota);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Like the previous test but create many files.</summary>
		/// <remarks>
		/// Like the previous test but create many files. This covers bugs where
		/// the quota adjustment is incorrect but it takes many files to accrue
		/// a big enough accounting error to violate the quota.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleFilesSmallerThanOneBlock()
		{
			Configuration conf = new HdfsConfiguration();
			int BlockSize = 6 * 1024;
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetBoolean(DFSConfigKeys.DfsWebhdfsEnabledKey, true);
			// Make it relinquish locks. When run serially, the result should
			// be identical.
			conf.SetInt(DFSConfigKeys.DfsContentSummaryLimitKey, 2);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			DFSAdmin admin = new DFSAdmin(conf);
			string nnAddr = conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey);
			string webhdfsuri = WebHdfsFileSystem.Scheme + "://" + nnAddr;
			System.Console.Out.WriteLine("webhdfsuri=" + webhdfsuri);
			FileSystem webhdfs = new Path(webhdfsuri).GetFileSystem(conf);
			try
			{
				Path dir = new Path("/test");
				bool exceededQuota = false;
				ContentSummary c;
				// 1kb file
				// 6kb block
				// 192kb quota
				int FileSize = 1024;
				int QuotaSize = 32 * (int)fs.GetDefaultBlockSize(dir);
				NUnit.Framework.Assert.AreEqual(6 * 1024, fs.GetDefaultBlockSize(dir));
				NUnit.Framework.Assert.AreEqual(192 * 1024, QuotaSize);
				// Create the dir and set the quota. We need to enable the quota before
				// writing the files as setting the quota afterwards will over-write
				// the cached disk space used for quota verification with the actual
				// amount used as calculated by INode#spaceConsumedInTree.
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				RunCommand(admin, false, "-setSpaceQuota", Sharpen.Extensions.ToString(QuotaSize)
					, dir.ToString());
				// We can create at most 59 files because block allocation is
				// conservative and initially assumes a full block is used, so we
				// need to leave at least 3 * BLOCK_SIZE free space when allocating
				// the last block: (58 * 3 * 1024) (3 * 6 * 1024) = 192kb
				for (int i = 0; i < 59; i++)
				{
					Path file = new Path("/test/test" + i);
					DFSTestUtil.CreateFile(fs, file, FileSize, (short)3, 1L);
					DFSTestUtil.WaitReplication(fs, file, (short)3);
				}
				// Should account for all 59 files (almost QUOTA_SIZE)
				c = fs.GetContentSummary(dir);
				CheckContentSummary(c, webhdfs.GetContentSummary(dir));
				NUnit.Framework.Assert.AreEqual("Invalid space consumed", 59 * FileSize * 3, c.GetSpaceConsumed
					());
				NUnit.Framework.Assert.AreEqual("Invalid space consumed", QuotaSize - (59 * FileSize
					 * 3), 3 * (fs.GetDefaultBlockSize(dir) - FileSize));
				// Now check that trying to create another file violates the quota
				try
				{
					Path file = new Path("/test/test59");
					DFSTestUtil.CreateFile(fs, file, FileSize, (short)3, 1L);
					DFSTestUtil.WaitReplication(fs, file, (short)3);
				}
				catch (QuotaExceededException)
				{
					exceededQuota = true;
				}
				NUnit.Framework.Assert.IsTrue("Quota not exceeded", exceededQuota);
				NUnit.Framework.Assert.AreEqual(2, cluster.GetNamesystem().GetFSDirectory().GetYieldCount
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
