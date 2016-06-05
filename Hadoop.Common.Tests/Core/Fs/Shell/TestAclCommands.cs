using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell
{
	public class TestAclCommands
	{
		private Configuration conf = null;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetfaclValidations()
		{
			NUnit.Framework.Assert.IsFalse("getfacl should fail without path", 0 == RunCommand
				(new string[] { "-getfacl" }));
			NUnit.Framework.Assert.IsFalse("getfacl should fail with extra argument", 0 == RunCommand
				(new string[] { "-getfacl", "/test", "extraArg" }));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSetfaclValidations()
		{
			NUnit.Framework.Assert.IsFalse("setfacl should fail without path", 0 == RunCommand
				(new string[] { "-setfacl" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail without aclSpec", 0 == RunCommand
				(new string[] { "-setfacl", "-m", "/path" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with conflicting options", 0 
				== RunCommand(new string[] { "-setfacl", "-m", "/path" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with extra arguments", 0 == RunCommand
				(new string[] { "-setfacl", "/path", "extra" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with extra arguments", 0 == RunCommand
				(new string[] { "-setfacl", "--set", "default:user::rwx", "/path", "extra" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with permissions for -x", 0 ==
				 RunCommand(new string[] { "-setfacl", "-x", "user:user1:rwx", "/path" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail ACL spec missing", 0 == RunCommand
				(new string[] { "-setfacl", "-m", string.Empty, "/path" }));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestSetfaclValidationsWithoutPermissions()
		{
			IList<AclEntry> parsedList = new AList<AclEntry>();
			try
			{
				parsedList = AclEntry.ParseAclSpec("user:user1:", true);
			}
			catch (ArgumentException)
			{
			}
			Assert.True(parsedList.Count == 0);
			NUnit.Framework.Assert.IsFalse("setfacl should fail with less arguments", 0 == RunCommand
				(new string[] { "-setfacl", "-m", "user:user1:", "/path" }));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMultipleAclSpecParsing()
		{
			IList<AclEntry> parsedList = AclEntry.ParseAclSpec("group::rwx,user:user1:rwx,user:user2:rw-,"
				 + "group:group1:rw-,default:group:group1:rw-", true);
			AclEntry basicAcl = new AclEntry.Builder().SetType(AclEntryType.Group).SetPermission
				(FsAction.All).Build();
			AclEntry user1Acl = new AclEntry.Builder().SetType(AclEntryType.User).SetPermission
				(FsAction.All).SetName("user1").Build();
			AclEntry user2Acl = new AclEntry.Builder().SetType(AclEntryType.User).SetPermission
				(FsAction.ReadWrite).SetName("user2").Build();
			AclEntry group1Acl = new AclEntry.Builder().SetType(AclEntryType.Group).SetPermission
				(FsAction.ReadWrite).SetName("group1").Build();
			AclEntry defaultAcl = new AclEntry.Builder().SetType(AclEntryType.Group).SetPermission
				(FsAction.ReadWrite).SetName("group1").SetScope(AclEntryScope.Default).Build();
			IList<AclEntry> expectedList = new AList<AclEntry>();
			expectedList.AddItem(basicAcl);
			expectedList.AddItem(user1Acl);
			expectedList.AddItem(user2Acl);
			expectedList.AddItem(group1Acl);
			expectedList.AddItem(defaultAcl);
			Assert.Equal("Parsed Acl not correct", expectedList, parsedList
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMultipleAclSpecParsingWithoutPermissions()
		{
			IList<AclEntry> parsedList = AclEntry.ParseAclSpec("user::,user:user1:,group::,group:group1:,mask::,other::,"
				 + "default:user:user1::,default:mask::", false);
			AclEntry owner = new AclEntry.Builder().SetType(AclEntryType.User).Build();
			AclEntry namedUser = new AclEntry.Builder().SetType(AclEntryType.User).SetName("user1"
				).Build();
			AclEntry group = new AclEntry.Builder().SetType(AclEntryType.Group).Build();
			AclEntry namedGroup = new AclEntry.Builder().SetType(AclEntryType.Group).SetName(
				"group1").Build();
			AclEntry mask = new AclEntry.Builder().SetType(AclEntryType.Mask).Build();
			AclEntry other = new AclEntry.Builder().SetType(AclEntryType.Other).Build();
			AclEntry defaultUser = new AclEntry.Builder().SetScope(AclEntryScope.Default).SetType
				(AclEntryType.User).SetName("user1").Build();
			AclEntry defaultMask = new AclEntry.Builder().SetScope(AclEntryScope.Default).SetType
				(AclEntryType.Mask).Build();
			IList<AclEntry> expectedList = new AList<AclEntry>();
			expectedList.AddItem(owner);
			expectedList.AddItem(namedUser);
			expectedList.AddItem(group);
			expectedList.AddItem(namedGroup);
			expectedList.AddItem(mask);
			expectedList.AddItem(other);
			expectedList.AddItem(defaultUser);
			expectedList.AddItem(defaultMask);
			Assert.Equal("Parsed Acl not correct", expectedList, parsedList
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLsNoRpcForGetAclStatus()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "stubfs:///");
			conf.SetClass("fs.stubfs.impl", typeof(TestAclCommands.StubFileSystem), typeof(FileSystem
				));
			conf.SetBoolean("stubfs.noRpcForGetAclStatus", true);
			Assert.Equal("ls must succeed even if getAclStatus RPC does not exist."
				, 0, ToolRunner.Run(conf, new FsShell(), new string[] { "-ls", "/" }));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLsAclsUnsupported()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsDefaultNameKey, "stubfs:///");
			conf.SetClass("fs.stubfs.impl", typeof(TestAclCommands.StubFileSystem), typeof(FileSystem
				));
			Assert.Equal("ls must succeed even if FileSystem does not implement ACLs."
				, 0, ToolRunner.Run(conf, new FsShell(), new string[] { "-ls", "/" }));
		}

		public class StubFileSystem : FileSystem
		{
			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
				)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
				, int bufferSize, short replication, long blockSize, Progressable progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Delete(Path f, bool recursive)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override AclStatus GetAclStatus(Path path)
			{
				if (GetConf().GetBoolean("stubfs.noRpcForGetAclStatus", false))
				{
					throw new RemoteException(typeof(RpcNoSuchMethodException).FullName, "test exception"
						);
				}
				return base.GetAclStatus(path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus GetFileStatus(Path f)
			{
				if (f.IsRoot())
				{
					return new FileStatus(0, true, 0, 0, 0, f);
				}
				return null;
			}

			public override URI GetUri()
			{
				return URI.Create("stubfs:///");
			}

			public override Path GetWorkingDirectory()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FileStatus[] ListStatus(Path f)
			{
				FsPermission perm = new FsPermission(FsAction.All, FsAction.ReadExecute, FsAction
					.ReadExecute);
				Path path = new Path("/foo");
				FileStatus stat = new FileStatus(1000, true, 3, 1000, 0, 0, perm, "owner", "group"
					, path);
				return new FileStatus[] { stat };
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Mkdirs(Path f, FsPermission permission)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override FSDataInputStream Open(Path f, int bufferSize)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool Rename(Path src, Path dst)
			{
				return false;
			}

			public override void SetWorkingDirectory(Path dir)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		private int RunCommand(string[] commands)
		{
			return ToolRunner.Run(conf, new FsShell(), commands);
		}
	}
}
