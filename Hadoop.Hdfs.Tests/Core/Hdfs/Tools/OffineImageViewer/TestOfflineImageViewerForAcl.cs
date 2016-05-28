using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>Tests OfflineImageViewer if the input fsimage has HDFS ACLs</summary>
	public class TestOfflineImageViewerForAcl
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestOfflineImageViewerForAcl
			));

		private static FilePath originalFsimage = null;

		internal static readonly Dictionary<string, AclStatus> writtenAcls = Maps.NewHashMap
			();

		// ACLs as set to dfs, to be compared with viewer's output
		/// <summary>Create a populated namespace for later testing.</summary>
		/// <remarks>
		/// Create a populated namespace for later testing. Save its contents to a
		/// data structure and store its fsimage location.
		/// We only want to generate the fsimage file once and use it for
		/// multiple tests.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void CreateOriginalFSImage()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new Configuration();
				conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				DistributedFileSystem hdfs = cluster.GetFileSystem();
				// Create a reasonable namespace with ACLs
				Path dir = new Path("/dirWithNoAcl");
				hdfs.Mkdirs(dir);
				writtenAcls[dir.ToString()] = hdfs.GetAclStatus(dir);
				dir = new Path("/dirWithDefaultAcl");
				hdfs.Mkdirs(dir);
				hdfs.SetAcl(dir, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Default
					, AclEntryType.User, FsAction.All), AclTestHelpers.AclEntry(AclEntryScope.Default
					, AclEntryType.User, "foo", FsAction.All), AclTestHelpers.AclEntry(AclEntryScope
					.Default, AclEntryType.Group, FsAction.ReadExecute), AclTestHelpers.AclEntry(AclEntryScope
					.Default, AclEntryType.Other, FsAction.None)));
				writtenAcls[dir.ToString()] = hdfs.GetAclStatus(dir);
				Path file = new Path("/noAcl");
				FSDataOutputStream o = hdfs.Create(file);
				o.Write(23);
				o.Close();
				writtenAcls[file.ToString()] = hdfs.GetAclStatus(file);
				file = new Path("/withAcl");
				o = hdfs.Create(file);
				o.Write(23);
				o.Close();
				hdfs.SetAcl(file, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access
					, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.
					Access, AclEntryType.User, "foo", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.Other, FsAction.None)));
				writtenAcls[file.ToString()] = hdfs.GetAclStatus(file);
				file = new Path("/withSeveralAcls");
				o = hdfs.Create(file);
				o.Write(23);
				o.Close();
				hdfs.SetAcl(file, Lists.NewArrayList(AclTestHelpers.AclEntry(AclEntryScope.Access
					, AclEntryType.User, FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope.
					Access, AclEntryType.User, "foo", FsAction.ReadWrite), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.User, "bar", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.Group, FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.Group, "group", FsAction.Read), AclTestHelpers.AclEntry(AclEntryScope
					.Access, AclEntryType.Other, FsAction.None)));
				writtenAcls[file.ToString()] = hdfs.GetAclStatus(file);
				// Write results to the fsimage file
				hdfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
				hdfs.SaveNamespace();
				// Determine the location of the fsimage file
				originalFsimage = FSImageTestUtil.FindLatestImageFile(FSImageTestUtil.GetFSImage(
					cluster.GetNameNode()).GetStorage().GetStorageDir(0));
				if (originalFsimage == null)
				{
					throw new RuntimeException("Didn't generate or can't find fsimage");
				}
				Log.Debug("original FS image file is " + originalFsimage);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void DeleteOriginalFSImage()
		{
			if (originalFsimage != null && originalFsimage.Exists())
			{
				originalFsimage.Delete();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWebImageViewerForAcl()
		{
			WebImageViewer viewer = new WebImageViewer(NetUtils.CreateSocketAddr("localhost:0"
				));
			try
			{
				viewer.InitServer(originalFsimage.GetAbsolutePath());
				int port = viewer.GetPort();
				// create a WebHdfsFileSystem instance
				URI uri = new URI("webhdfs://localhost:" + port.ToString());
				Configuration conf = new Configuration();
				WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)FileSystem.Get(uri, conf);
				// GETACLSTATUS operation to a directory without ACL
				AclStatus acl = webhdfs.GetAclStatus(new Path("/dirWithNoAcl"));
				NUnit.Framework.Assert.AreEqual(writtenAcls["/dirWithNoAcl"], acl);
				// GETACLSTATUS operation to a directory with a default ACL
				acl = webhdfs.GetAclStatus(new Path("/dirWithDefaultAcl"));
				NUnit.Framework.Assert.AreEqual(writtenAcls["/dirWithDefaultAcl"], acl);
				// GETACLSTATUS operation to a file without ACL
				acl = webhdfs.GetAclStatus(new Path("/noAcl"));
				NUnit.Framework.Assert.AreEqual(writtenAcls["/noAcl"], acl);
				// GETACLSTATUS operation to a file with a ACL
				acl = webhdfs.GetAclStatus(new Path("/withAcl"));
				NUnit.Framework.Assert.AreEqual(writtenAcls["/withAcl"], acl);
				// GETACLSTATUS operation to a file with several ACL entries
				acl = webhdfs.GetAclStatus(new Path("/withSeveralAcls"));
				NUnit.Framework.Assert.AreEqual(writtenAcls["/withSeveralAcls"], acl);
				// GETACLSTATUS operation to a invalid path
				Uri url = new Uri("http://localhost:" + port + "/webhdfs/v1/invalid/?op=GETACLSTATUS"
					);
				HttpURLConnection connection = (HttpURLConnection)url.OpenConnection();
				connection.SetRequestMethod("GET");
				connection.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpNotFound, connection.GetResponseCode
					());
			}
			finally
			{
				// shutdown the viewer
				viewer.Close();
			}
		}
	}
}
