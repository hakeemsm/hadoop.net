using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestAfsCheckPath
	{
		private static int DefaultPort = 1234;

		private static int OtherPort = 4321;

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckPathWithNoPorts()
		{
			URI uri = new URI("dummy://dummy-host");
			AbstractFileSystem afs = new TestAfsCheckPath.DummyFileSystem(uri);
			afs.CheckPath(new Path("dummy://dummy-host"));
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckPathWithDefaultPort()
		{
			URI uri = new URI("dummy://dummy-host:" + DefaultPort);
			AbstractFileSystem afs = new TestAfsCheckPath.DummyFileSystem(uri);
			afs.CheckPath(new Path("dummy://dummy-host:" + DefaultPort));
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckPathWithTheSameNonDefaultPort()
		{
			URI uri = new URI("dummy://dummy-host:" + OtherPort);
			AbstractFileSystem afs = new TestAfsCheckPath.DummyFileSystem(uri);
			afs.CheckPath(new Path("dummy://dummy-host:" + OtherPort));
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual void TestCheckPathWithDifferentPorts()
		{
			URI uri = new URI("dummy://dummy-host:" + DefaultPort);
			AbstractFileSystem afs = new TestAfsCheckPath.DummyFileSystem(uri);
			afs.CheckPath(new Path("dummy://dummy-host:" + OtherPort));
		}

		private class DummyFileSystem : AbstractFileSystem
		{
			/// <exception cref="Sharpen.URISyntaxException"/>
			public DummyFileSystem(URI uri)
				: base(uri, "dummy", true, DefaultPort)
			{
			}

			public override int GetUriDefaultPort()
			{
				return DefaultPort;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> flag
				, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
				, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
			/// <exception cref="System.IO.FileNotFoundException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path f, bool recursive)
			{
				// deliberately empty
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override BlockLocation[] GetFileBlockLocations(Path f, long start, long len
				)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileChecksum GetFileChecksum(Path f)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FsStatus GetFsStatus()
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FsServerDefaults GetServerDefaults()
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Mkdir(Path dir, FsPermission permission, bool createParent)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Open(Path f, int bufferSize)
			{
				// deliberately empty
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RenameInternal(Path src, Path dst)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override void SetOwner(Path f, string username, string groupname)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override void SetPermission(Path f, FsPermission permission)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override bool SetReplication(Path f, short replication)
			{
				// deliberately empty
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetTimes(Path f, long mtime, long atime)
			{
			}

			// deliberately empty
			/// <exception cref="System.IO.IOException"/>
			public override void SetVerifyChecksum(bool verifyChecksum)
			{
			}
			// deliberately empty
		}
	}
}
