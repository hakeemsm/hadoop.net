using System.Collections.Generic;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestINodeAttributeProvider
	{
		private MiniDFSCluster miniDFS;

		private static readonly ICollection<string> Called = new HashSet<string>();

		public class MyAuthorizationProvider : INodeAttributeProvider
		{
			public class MyAccessControlEnforcer : INodeAttributeProvider.AccessControlEnforcer
			{
				/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
				public virtual void CheckPermission(string fsOwner, string supergroup, UserGroupInformation
					 ugi, INodeAttributes[] inodeAttrs, INode[] inodes, byte[][] pathByNameArr, int 
					snapshotId, string path, int ancestorIndex, bool doCheckOwner, FsAction ancestorAccess
					, FsAction parentAccess, FsAction access, FsAction subAccess, bool ignoreEmptyDir
					)
				{
					Called.AddItem("checkPermission|" + ancestorAccess + "|" + parentAccess + "|" + access
						);
				}
			}

			public override void Start()
			{
				Called.AddItem("start");
			}

			public override void Stop()
			{
				Called.AddItem("stop");
			}

			public override INodeAttributes GetAttributes(string[] pathElements, INodeAttributes
				 inode)
			{
				Called.AddItem("getAttributes");
				bool useDefault = UseDefault(pathElements);
				return new _INodeAttributes_80(inode, useDefault);
			}

			private sealed class _INodeAttributes_80 : INodeAttributes
			{
				public _INodeAttributes_80(INodeAttributes inode, bool useDefault)
				{
					this.inode = inode;
					this.useDefault = useDefault;
				}

				public override bool IsDirectory()
				{
					return inode.IsDirectory();
				}

				public override byte[] GetLocalNameBytes()
				{
					return inode.GetLocalNameBytes();
				}

				public override string GetUserName()
				{
					return (useDefault) ? inode.GetUserName() : "foo";
				}

				public override string GetGroupName()
				{
					return (useDefault) ? inode.GetGroupName() : "bar";
				}

				public override FsPermission GetFsPermission()
				{
					return (useDefault) ? inode.GetFsPermission() : new FsPermission(this.GetFsPermissionShort
						());
				}

				public override short GetFsPermissionShort()
				{
					return (useDefault) ? inode.GetFsPermissionShort() : (short)this.GetPermissionLong
						();
				}

				public override long GetPermissionLong()
				{
					return (useDefault) ? inode.GetPermissionLong() : 0x1f8;
				}

				public override AclFeature GetAclFeature()
				{
					AclFeature f;
					if (useDefault)
					{
						f = inode.GetAclFeature();
					}
					else
					{
						AclEntry acl = new AclEntry.Builder().SetType(AclEntryType.Group).SetPermission(FsAction
							.All).SetName("xxx").Build();
						f = new AclFeature(AclEntryStatusFormat.ToInt(Lists.NewArrayList(acl)));
					}
					return f;
				}

				public override XAttrFeature GetXAttrFeature()
				{
					XAttrFeature x;
					if (useDefault)
					{
						x = inode.GetXAttrFeature();
					}
					else
					{
						x = new XAttrFeature(ImmutableList.CopyOf(Lists.NewArrayList(new XAttr.Builder().
							SetName("test").SetValue(new byte[] { 1, 2 }).Build())));
					}
					return x;
				}

				public override long GetModificationTime()
				{
					return (useDefault) ? inode.GetModificationTime() : 0;
				}

				public override long GetAccessTime()
				{
					return (useDefault) ? inode.GetAccessTime() : 0;
				}

				private readonly INodeAttributes inode;

				private readonly bool useDefault;
			}

			public override INodeAttributeProvider.AccessControlEnforcer GetExternalAccessControlEnforcer
				(INodeAttributeProvider.AccessControlEnforcer deafultEnforcer)
			{
				return new TestINodeAttributeProvider.MyAuthorizationProvider.MyAccessControlEnforcer
					();
			}

			private bool UseDefault(string[] pathElements)
			{
				return (pathElements.Length < 2) || !(pathElements[0].Equals("user") && pathElements
					[1].Equals("authz"));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUp()
		{
			Called.Clear();
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNamenodeInodeAttributesProviderKey, typeof(TestINodeAttributeProvider.MyAuthorizationProvider
				).FullName);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
			miniDFS = new MiniDFSCluster.Builder(conf).Build();
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void CleanUp()
		{
			Called.Clear();
			if (miniDFS != null)
			{
				miniDFS.Shutdown();
			}
			NUnit.Framework.Assert.IsTrue(Called.Contains("stop"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationToProvider()
		{
			NUnit.Framework.Assert.IsTrue(Called.Contains("start"));
			FileSystem fs = FileSystem.Get(miniDFS.GetConfiguration(0));
			fs.Mkdirs(new Path("/tmp"));
			fs.SetPermission(new Path("/tmp"), new FsPermission((short)0x1ff));
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("u1", new string
				[] { "g1" });
			ugi.DoAs(new _PrivilegedExceptionAction_201(this));
		}

		private sealed class _PrivilegedExceptionAction_201 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_201(TestINodeAttributeProvider _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				FileSystem fs = FileSystem.Get(this._enclosing.miniDFS.GetConfiguration(0));
				TestINodeAttributeProvider.Called.Clear();
				fs.Mkdirs(new Path("/tmp/foo"));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("getAttributes"
					));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("checkPermission|null|null|null"
					));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("checkPermission|WRITE|null|null"
					));
				TestINodeAttributeProvider.Called.Clear();
				fs.ListStatus(new Path("/tmp/foo"));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("getAttributes"
					));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("checkPermission|null|null|READ_EXECUTE"
					));
				TestINodeAttributeProvider.Called.Clear();
				fs.GetAclStatus(new Path("/tmp/foo"));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("getAttributes"
					));
				NUnit.Framework.Assert.IsTrue(TestINodeAttributeProvider.Called.Contains("checkPermission|null|null|null"
					));
				return null;
			}

			private readonly TestINodeAttributeProvider _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCustomProvider()
		{
			FileSystem fs = FileSystem.Get(miniDFS.GetConfiguration(0));
			fs.Mkdirs(new Path("/user/xxx"));
			FileStatus status = fs.GetFileStatus(new Path("/user/xxx"));
			NUnit.Framework.Assert.AreEqual(Runtime.GetProperty("user.name"), status.GetOwner
				());
			NUnit.Framework.Assert.AreEqual("supergroup", status.GetGroup());
			NUnit.Framework.Assert.AreEqual(new FsPermission((short)0x1ed), status.GetPermission
				());
			fs.Mkdirs(new Path("/user/authz"));
			Path p = new Path("/user/authz");
			status = fs.GetFileStatus(p);
			NUnit.Framework.Assert.AreEqual("foo", status.GetOwner());
			NUnit.Framework.Assert.AreEqual("bar", status.GetGroup());
			NUnit.Framework.Assert.AreEqual(new FsPermission((short)0x1f8), status.GetPermission
				());
			AclStatus aclStatus = fs.GetAclStatus(p);
			NUnit.Framework.Assert.AreEqual(1, aclStatus.GetEntries().Count);
			NUnit.Framework.Assert.AreEqual(AclEntryType.Group, aclStatus.GetEntries()[0].GetType
				());
			NUnit.Framework.Assert.AreEqual("xxx", aclStatus.GetEntries()[0].GetName());
			NUnit.Framework.Assert.AreEqual(FsAction.All, aclStatus.GetEntries()[0].GetPermission
				());
			IDictionary<string, byte[]> xAttrs = fs.GetXAttrs(p);
			NUnit.Framework.Assert.IsTrue(xAttrs.Contains("user.test"));
			NUnit.Framework.Assert.AreEqual(2, xAttrs["user.test"].Length);
		}
	}
}
