using System.IO;
using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Tests that the configuration flag that controls support for XAttrs is off
	/// and causes all attempted operations related to XAttrs to fail.
	/// </summary>
	/// <remarks>
	/// Tests that the configuration flag that controls support for XAttrs is off
	/// and causes all attempted operations related to XAttrs to fail.  The
	/// NameNode can still load XAttrs from fsimage or edits.
	/// </remarks>
	public class TestXAttrConfigFlag
	{
		private static readonly Path Path = new Path("/path");

		private MiniDFSCluster cluster;

		private DistributedFileSystem fs;

		[Rule]
		public ExpectedException exception = ExpectedException.None();

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Shutdown()
		{
			IOUtils.Cleanup(null, fs);
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSetXAttr()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.SetXAttr(Path, "user.foo", null);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetXAttrs()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.GetXAttrs(Path);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveXAttr()
		{
			InitCluster(true, false);
			fs.Mkdirs(Path);
			ExpectException();
			fs.RemoveXAttr(Path, "user.foo");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEditLog()
		{
			// With XAttrs enabled, set an XAttr.
			InitCluster(true, true);
			fs.Mkdirs(Path);
			fs.SetXAttr(Path, "user.foo", null);
			// Restart with XAttrs disabled.  Expect successful restart.
			Restart(false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsImage()
		{
			// With XAttrs enabled, set an XAttr.
			InitCluster(true, true);
			fs.Mkdirs(Path);
			fs.SetXAttr(Path, "user.foo", null);
			// Save a new checkpoint and restart with XAttrs still enabled.
			Restart(true, true);
			// Restart with XAttrs disabled.  Expect successful restart.
			Restart(false, false);
		}

		/// <summary>
		/// We expect an IOException, and we want the exception text to state the
		/// configuration key that controls XAttr support.
		/// </summary>
		private void ExpectException()
		{
			exception.Expect(typeof(IOException));
			exception.ExpectMessage(DFSConfigKeys.DfsNamenodeXattrsEnabledKey);
		}

		/// <summary>Initialize the cluster, wait for it to become active, and get FileSystem.
		/// 	</summary>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <param name="xattrsEnabled">if true, XAttr support is enabled</param>
		/// <exception cref="System.Exception">if any step fails</exception>
		private void InitCluster(bool format, bool xattrsEnabled)
		{
			Configuration conf = new Configuration();
			// not explicitly setting to false, should be false by default
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, xattrsEnabled);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Format(format).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <summary>Restart the cluster, optionally saving a new checkpoint.</summary>
		/// <param name="checkpoint">boolean true to save a new checkpoint</param>
		/// <param name="xattrsEnabled">if true, XAttr support is enabled</param>
		/// <exception cref="System.Exception">if restart fails</exception>
		private void Restart(bool checkpoint, bool xattrsEnabled)
		{
			NameNode nameNode = cluster.GetNameNode();
			if (checkpoint)
			{
				NameNodeAdapter.EnterSafeMode(nameNode, false);
				NameNodeAdapter.SaveNamespace(nameNode);
			}
			Shutdown();
			InitCluster(false, xattrsEnabled);
		}
	}
}
