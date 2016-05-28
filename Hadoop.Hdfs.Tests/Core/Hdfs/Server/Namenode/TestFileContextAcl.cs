using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Tests for ACL operation through FileContext APIs</summary>
	public class TestFileContextAcl : FSAclBaseTest
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			conf = new Configuration();
			StartCluster();
		}

		/// <exception cref="System.Exception"/>
		protected internal override FileSystem CreateFileSystem()
		{
			TestFileContextAcl.FileContextFS fcFs = new TestFileContextAcl.FileContextFS();
			fcFs.Initialize(FileSystem.GetDefaultUri(conf), conf);
			return fcFs;
		}

		public class FileContextFS : DistributedFileSystem
		{
			private FileContext fc;

			/*
			* To Re-use the FSAclBaseTest's testcases, creating a filesystem
			* implementation which works based on fileContext. In this only overriding
			* acl related methods, other operations will happen using normal filesystem
			* itself which is out of scope for this test
			*/
			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI uri, Configuration conf)
			{
				base.Initialize(uri, conf);
				fc = FileContext.GetFileContext(conf);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ModifyAclEntries(Path path, IList<AclEntry> aclSpec)
			{
				fc.ModifyAclEntries(path, aclSpec);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveAclEntries(Path path, IList<AclEntry> aclSpec)
			{
				fc.RemoveAclEntries(path, aclSpec);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveDefaultAcl(Path path)
			{
				fc.RemoveDefaultAcl(path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void RemoveAcl(Path path)
			{
				fc.RemoveAcl(path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetAcl(Path path, IList<AclEntry> aclSpec)
			{
				fc.SetAcl(path, aclSpec);
			}

			/// <exception cref="System.IO.IOException"/>
			public override AclStatus GetAclStatus(Path path)
			{
				return fc.GetAclStatus(path);
			}
		}
	}
}
