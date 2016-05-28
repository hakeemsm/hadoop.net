using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestClusterId
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestClusterId));

		internal FilePath hdfsDir;

		internal Configuration config;

		/// <exception cref="System.IO.IOException"/>
		private string GetClusterId(Configuration config)
		{
			// see if cluster id not empty.
			ICollection<URI> dirsToFormat = FSNamesystem.GetNamespaceDirs(config);
			IList<URI> editsToFormat = FSNamesystem.GetNamespaceEditsDirs(config);
			FSImage fsImage = new FSImage(config, dirsToFormat, editsToFormat);
			IEnumerator<Storage.StorageDirectory> sdit = fsImage.GetStorage().DirIterator(NNStorage.NameNodeDirType
				.Image);
			Storage.StorageDirectory sd = sdit.Next();
			Properties props = Storage.ReadPropertiesFile(sd.GetVersionFile());
			string cid = props.GetProperty("clusterID");
			Log.Info("successfully formated : sd=" + sd.GetCurrentDir() + ";cid=" + cid);
			return cid;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			ExitUtil.DisableSystemExit();
			string baseDir = PathUtils.GetTestDirName(GetType());
			hdfsDir = new FilePath(baseDir, "dfs/name");
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete test directory '" + hdfsDir + "'");
			}
			Log.Info("hdfsdir is " + hdfsDir.GetAbsolutePath());
			// as some tests might change these values we reset them to defaults before
			// every test
			HdfsServerConstants.StartupOption.Format.SetForceFormat(false);
			HdfsServerConstants.StartupOption.Format.SetInteractiveFormat(true);
			config = new Configuration();
			config.Set(DFSConfigKeys.DfsNamenodeNameDirKey, hdfsDir.GetPath());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not tearDown test directory '" + hdfsDir + "'");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatClusterIdOption()
		{
			// 1. should format without cluster id
			//StartupOption.FORMAT.setClusterId("");
			NameNode.Format(config);
			// see if cluster id not empty.
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
			// 2. successful format with given clusterid
			HdfsServerConstants.StartupOption.Format.SetClusterId("mycluster");
			NameNode.Format(config);
			// see if cluster id matches with given clusterid.
			cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("ClusterId didn't match", cid.Equals("mycluster"));
			// 3. format without any clusterid again. It should generate new
			//clusterid.
			HdfsServerConstants.StartupOption.Format.SetClusterId(string.Empty);
			NameNode.Format(config);
			string newCid = GetClusterId(config);
			NUnit.Framework.Assert.IsFalse("ClusterId should not be the same", newCid.Equals(
				cid));
		}

		/// <summary>Test namenode format with -format option.</summary>
		/// <remarks>Test namenode format with -format option. Format should succeed.</remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormat()
		{
			string[] argv = new string[] { "-format" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
		}

		/// <summary>
		/// Test namenode format with -format option when an empty name directory
		/// exists.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format option when an empty name directory
		/// exists. Format should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithEmptyDir()
		{
			if (!hdfsDir.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + hdfsDir.GetPath());
			}
			string[] argv = new string[] { "-format" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
		}

		/// <summary>
		/// Test namenode format with -format -force options when name directory
		/// exists.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format -force options when name directory
		/// exists. Format should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithForce()
		{
			if (!hdfsDir.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + hdfsDir.GetPath());
			}
			string[] argv = new string[] { "-format", "-force" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
		}

		/// <summary>
		/// Test namenode format with -format -force -clusterid option when name
		/// directory exists.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format -force -clusterid option when name
		/// directory exists. Format should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithForceAndClusterId()
		{
			if (!hdfsDir.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + hdfsDir.GetPath());
			}
			string myId = "testFormatWithForceAndClusterId";
			string[] argv = new string[] { "-format", "-force", "-clusterid", myId };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			string cId = GetClusterId(config);
			NUnit.Framework.Assert.AreEqual("ClusterIds do not match", myId, cId);
		}

		/// <summary>Test namenode format with -clusterid -force option.</summary>
		/// <remarks>
		/// Test namenode format with -clusterid -force option. Format command should
		/// fail as no cluster id was provided.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithInvalidClusterIdOption()
		{
			string[] argv = new string[] { "-format", "-clusterid", "-force" };
			TextWriter origErr = System.Console.Error;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextWriter stdErr = new TextWriter(baos);
			Runtime.SetErr(stdErr);
			NameNode.CreateNameNode(argv, config);
			// Check if usage is printed
			NUnit.Framework.Assert.IsTrue(baos.ToString("UTF-8").Contains("Usage: java NameNode"
				));
			Runtime.SetErr(origErr);
			// check if the version file does not exists.
			FilePath version = new FilePath(hdfsDir, "current/VERSION");
			NUnit.Framework.Assert.IsFalse("Check version should not exist", version.Exists()
				);
		}

		/// <summary>Test namenode format with -format -clusterid options.</summary>
		/// <remarks>
		/// Test namenode format with -format -clusterid options. Format should fail
		/// was no clusterid was sent.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithNoClusterIdOption()
		{
			string[] argv = new string[] { "-format", "-clusterid" };
			TextWriter origErr = System.Console.Error;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextWriter stdErr = new TextWriter(baos);
			Runtime.SetErr(stdErr);
			NameNode.CreateNameNode(argv, config);
			// Check if usage is printed
			NUnit.Framework.Assert.IsTrue(baos.ToString("UTF-8").Contains("Usage: java NameNode"
				));
			Runtime.SetErr(origErr);
			// check if the version file does not exists.
			FilePath version = new FilePath(hdfsDir, "current/VERSION");
			NUnit.Framework.Assert.IsFalse("Check version should not exist", version.Exists()
				);
		}

		/// <summary>Test namenode format with -format -clusterid and empty clusterid.</summary>
		/// <remarks>
		/// Test namenode format with -format -clusterid and empty clusterid. Format
		/// should fail as no valid if was provided.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithEmptyClusterIdOption()
		{
			string[] argv = new string[] { "-format", "-clusterid", string.Empty };
			TextWriter origErr = System.Console.Error;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextWriter stdErr = new TextWriter(baos);
			Runtime.SetErr(stdErr);
			NameNode.CreateNameNode(argv, config);
			// Check if usage is printed
			NUnit.Framework.Assert.IsTrue(baos.ToString("UTF-8").Contains("Usage: java NameNode"
				));
			Runtime.SetErr(origErr);
			// check if the version file does not exists.
			FilePath version = new FilePath(hdfsDir, "current/VERSION");
			NUnit.Framework.Assert.IsFalse("Check version should not exist", version.Exists()
				);
		}

		/// <summary>
		/// Test namenode format with -format -nonInteractive options when a non empty
		/// name directory exists.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format -nonInteractive options when a non empty
		/// name directory exists. Format should not succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithNonInteractive()
		{
			// we check for a non empty dir, so create a child path
			FilePath data = new FilePath(hdfsDir, "file");
			if (!data.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + data.GetPath());
			}
			string[] argv = new string[] { "-format", "-nonInteractive" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have been aborted with exit code 1"
					, 1, e.status);
			}
			// check if the version file does not exists.
			FilePath version = new FilePath(hdfsDir, "current/VERSION");
			NUnit.Framework.Assert.IsFalse("Check version should not exist", version.Exists()
				);
		}

		/// <summary>
		/// Test namenode format with -format -nonInteractive options when name
		/// directory does not exist.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format -nonInteractive options when name
		/// directory does not exist. Format should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithNonInteractiveNameDirDoesNotExit()
		{
			string[] argv = new string[] { "-format", "-nonInteractive" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
		}

		/// <summary>Test namenode format with -force -nonInteractive -force option.</summary>
		/// <remarks>
		/// Test namenode format with -force -nonInteractive -force option. Format
		/// should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithNonInteractiveAndForce()
		{
			if (!hdfsDir.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + hdfsDir.GetPath());
			}
			string[] argv = new string[] { "-format", "-nonInteractive", "-force" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
		}

		/// <summary>
		/// Test namenode format with -format option when a non empty name directory
		/// exists.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format option when a non empty name directory
		/// exists. Enter Y when prompted and the format should succeed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithoutForceEnterYes()
		{
			// we check for a non empty dir, so create a child path
			FilePath data = new FilePath(hdfsDir, "file");
			if (!data.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + data.GetPath());
			}
			// capture the input stream
			InputStream origIn = Runtime.@in;
			ByteArrayInputStream bins = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString
				("Y\n"));
			Runtime.SetIn(bins);
			string[] argv = new string[] { "-format" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should have succeeded", 0, e.status);
			}
			Runtime.SetIn(origIn);
			string cid = GetClusterId(config);
			NUnit.Framework.Assert.IsTrue("Didn't get new ClusterId", (cid != null && !cid.Equals
				(string.Empty)));
		}

		/// <summary>
		/// Test namenode format with -format option when a non empty name directory
		/// exists.
		/// </summary>
		/// <remarks>
		/// Test namenode format with -format option when a non empty name directory
		/// exists. Enter N when prompted and format should be aborted.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFormatWithoutForceEnterNo()
		{
			// we check for a non empty dir, so create a child path
			FilePath data = new FilePath(hdfsDir, "file");
			if (!data.Mkdirs())
			{
				NUnit.Framework.Assert.Fail("Failed to create dir " + data.GetPath());
			}
			// capture the input stream
			InputStream origIn = Runtime.@in;
			ByteArrayInputStream bins = new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString
				("N\n"));
			Runtime.SetIn(bins);
			string[] argv = new string[] { "-format" };
			try
			{
				NameNode.CreateNameNode(argv, config);
				NUnit.Framework.Assert.Fail("createNameNode() did not call System.exit()");
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual("Format should not have succeeded", 1, e.status);
			}
			Runtime.SetIn(origIn);
			// check if the version file does not exists.
			FilePath version = new FilePath(hdfsDir, "current/VERSION");
			NUnit.Framework.Assert.IsFalse("Check version should not exist", version.Exists()
				);
		}
	}
}
