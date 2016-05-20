using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestAfsCheckPath
	{
		private static int DEFAULT_PORT = 1234;

		private static int OTHER_PORT = 4321;

		/// <exception cref="java.net.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void testCheckPathWithNoPorts()
		{
			java.net.URI uri = new java.net.URI("dummy://dummy-host");
			org.apache.hadoop.fs.AbstractFileSystem afs = new org.apache.hadoop.fs.TestAfsCheckPath.DummyFileSystem
				(uri);
			afs.checkPath(new org.apache.hadoop.fs.Path("dummy://dummy-host"));
		}

		/// <exception cref="java.net.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void testCheckPathWithDefaultPort()
		{
			java.net.URI uri = new java.net.URI("dummy://dummy-host:" + DEFAULT_PORT);
			org.apache.hadoop.fs.AbstractFileSystem afs = new org.apache.hadoop.fs.TestAfsCheckPath.DummyFileSystem
				(uri);
			afs.checkPath(new org.apache.hadoop.fs.Path("dummy://dummy-host:" + DEFAULT_PORT)
				);
		}

		/// <exception cref="java.net.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void testCheckPathWithTheSameNonDefaultPort()
		{
			java.net.URI uri = new java.net.URI("dummy://dummy-host:" + OTHER_PORT);
			org.apache.hadoop.fs.AbstractFileSystem afs = new org.apache.hadoop.fs.TestAfsCheckPath.DummyFileSystem
				(uri);
			afs.checkPath(new org.apache.hadoop.fs.Path("dummy://dummy-host:" + OTHER_PORT));
		}

		/// <exception cref="java.net.URISyntaxException"/>
		public virtual void testCheckPathWithDifferentPorts()
		{
			java.net.URI uri = new java.net.URI("dummy://dummy-host:" + DEFAULT_PORT);
			org.apache.hadoop.fs.AbstractFileSystem afs = new org.apache.hadoop.fs.TestAfsCheckPath.DummyFileSystem
				(uri);
			afs.checkPath(new org.apache.hadoop.fs.Path("dummy://dummy-host:" + OTHER_PORT));
		}

		private class DummyFileSystem : org.apache.hadoop.fs.AbstractFileSystem
		{
			/// <exception cref="java.net.URISyntaxException"/>
			public DummyFileSystem(java.net.URI uri)
				: base(uri, "dummy", true, DEFAULT_PORT)
			{
			}

			public override int getUriDefaultPort()
			{
				return DEFAULT_PORT;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
				 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> flag, org.apache.hadoop.fs.permission.FsPermission
				 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
				)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="org.apache.hadoop.security.AccessControlException"/>
			/// <exception cref="java.io.FileNotFoundException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
			{
				// deliberately empty
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.BlockLocation[] getFileBlockLocations(org.apache.hadoop.fs.Path
				 f, long start, long len)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileChecksum getFileChecksum(org.apache.hadoop.fs.Path
				 f)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
				 f)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FsStatus getFsStatus()
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FsServerDefaults getServerDefaults()
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void mkdir(org.apache.hadoop.fs.Path dir, org.apache.hadoop.fs.permission.FsPermission
				 permission, bool createParent)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
				 f, int bufferSize)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override void setOwner(org.apache.hadoop.fs.Path f, string username, string
				 groupname)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override void setPermission(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 permission)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override bool setReplication(org.apache.hadoop.fs.Path f, short replication
				)
			{
				// deliberately empty
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void setTimes(org.apache.hadoop.fs.Path f, long mtime, long atime
				)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override void setVerifyChecksum(bool verifyChecksum)
			{
			}
			// deliberately empty
		}
	}
}
