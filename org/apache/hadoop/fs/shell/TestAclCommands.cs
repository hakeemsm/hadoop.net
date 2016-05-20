using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	public class TestAclCommands
	{
		private org.apache.hadoop.conf.Configuration conf = null;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetfaclValidations()
		{
			NUnit.Framework.Assert.IsFalse("getfacl should fail without path", 0 == runCommand
				(new string[] { "-getfacl" }));
			NUnit.Framework.Assert.IsFalse("getfacl should fail with extra argument", 0 == runCommand
				(new string[] { "-getfacl", "/test", "extraArg" }));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSetfaclValidations()
		{
			NUnit.Framework.Assert.IsFalse("setfacl should fail without path", 0 == runCommand
				(new string[] { "-setfacl" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail without aclSpec", 0 == runCommand
				(new string[] { "-setfacl", "-m", "/path" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with conflicting options", 0 
				== runCommand(new string[] { "-setfacl", "-m", "/path" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with extra arguments", 0 == runCommand
				(new string[] { "-setfacl", "/path", "extra" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with extra arguments", 0 == runCommand
				(new string[] { "-setfacl", "--set", "default:user::rwx", "/path", "extra" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail with permissions for -x", 0 ==
				 runCommand(new string[] { "-setfacl", "-x", "user:user1:rwx", "/path" }));
			NUnit.Framework.Assert.IsFalse("setfacl should fail ACL spec missing", 0 == runCommand
				(new string[] { "-setfacl", "-m", string.Empty, "/path" }));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testSetfaclValidationsWithoutPermissions()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> parsedList
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.permission.AclEntry>
				();
			try
			{
				parsedList = org.apache.hadoop.fs.permission.AclEntry.parseAclSpec("user:user1:", 
					true);
			}
			catch (System.ArgumentException)
			{
			}
			NUnit.Framework.Assert.IsTrue(parsedList.Count == 0);
			NUnit.Framework.Assert.IsFalse("setfacl should fail with less arguments", 0 == runCommand
				(new string[] { "-setfacl", "-m", "user:user1:", "/path" }));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMultipleAclSpecParsing()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> parsedList
				 = org.apache.hadoop.fs.permission.AclEntry.parseAclSpec("group::rwx,user:user1:rwx,user:user2:rw-,"
				 + "group:group1:rw-,default:group:group1:rw-", true);
			org.apache.hadoop.fs.permission.AclEntry basicAcl = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.GROUP).setPermission(org.apache.hadoop.fs.permission.FsAction
				.ALL).build();
			org.apache.hadoop.fs.permission.AclEntry user1Acl = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.USER).setPermission(org.apache.hadoop.fs.permission.FsAction
				.ALL).setName("user1").build();
			org.apache.hadoop.fs.permission.AclEntry user2Acl = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.USER).setPermission(org.apache.hadoop.fs.permission.FsAction
				.READ_WRITE).setName("user2").build();
			org.apache.hadoop.fs.permission.AclEntry group1Acl = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.GROUP).setPermission(org.apache.hadoop.fs.permission.FsAction
				.READ_WRITE).setName("group1").build();
			org.apache.hadoop.fs.permission.AclEntry defaultAcl = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.GROUP).setPermission(org.apache.hadoop.fs.permission.FsAction
				.READ_WRITE).setName("group1").setScope(org.apache.hadoop.fs.permission.AclEntryScope
				.DEFAULT).build();
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> expectedList
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.permission.AclEntry>
				();
			expectedList.add(basicAcl);
			expectedList.add(user1Acl);
			expectedList.add(user2Acl);
			expectedList.add(group1Acl);
			expectedList.add(defaultAcl);
			NUnit.Framework.Assert.AreEqual("Parsed Acl not correct", expectedList, parsedList
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMultipleAclSpecParsingWithoutPermissions()
		{
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> parsedList
				 = org.apache.hadoop.fs.permission.AclEntry.parseAclSpec("user::,user:user1:,group::,group:group1:,mask::,other::,"
				 + "default:user:user1::,default:mask::", false);
			org.apache.hadoop.fs.permission.AclEntry owner = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.USER).build();
			org.apache.hadoop.fs.permission.AclEntry namedUser = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.USER).setName("user1").build
				();
			org.apache.hadoop.fs.permission.AclEntry group = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.GROUP).build();
			org.apache.hadoop.fs.permission.AclEntry namedGroup = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.GROUP).setName("group1")
				.build();
			org.apache.hadoop.fs.permission.AclEntry mask = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.MASK).build();
			org.apache.hadoop.fs.permission.AclEntry other = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setType(org.apache.hadoop.fs.permission.AclEntryType.OTHER).build();
			org.apache.hadoop.fs.permission.AclEntry defaultUser = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setScope(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT).setType(org.apache.hadoop.fs.permission.AclEntryType
				.USER).setName("user1").build();
			org.apache.hadoop.fs.permission.AclEntry defaultMask = new org.apache.hadoop.fs.permission.AclEntry.Builder
				().setScope(org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT).setType(org.apache.hadoop.fs.permission.AclEntryType
				.MASK).build();
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> expectedList
				 = new System.Collections.Generic.List<org.apache.hadoop.fs.permission.AclEntry>
				();
			expectedList.add(owner);
			expectedList.add(namedUser);
			expectedList.add(group);
			expectedList.add(namedGroup);
			expectedList.add(mask);
			expectedList.add(other);
			expectedList.add(defaultUser);
			expectedList.add(defaultMask);
			NUnit.Framework.Assert.AreEqual("Parsed Acl not correct", expectedList, parsedList
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLsNoRpcForGetAclStatus()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "stubfs:///"
				);
			conf.setClass("fs.stubfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestAclCommands.StubFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			conf.setBoolean("stubfs.noRpcForGetAclStatus", true);
			NUnit.Framework.Assert.AreEqual("ls must succeed even if getAclStatus RPC does not exist."
				, 0, org.apache.hadoop.util.ToolRunner.run(conf, new org.apache.hadoop.fs.FsShell
				(), new string[] { "-ls", "/" }));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLsAclsUnsupported()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "stubfs:///"
				);
			conf.setClass("fs.stubfs.impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.TestAclCommands.StubFileSystem
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FileSystem)));
			NUnit.Framework.Assert.AreEqual("ls must succeed even if FileSystem does not implement ACLs."
				, 0, org.apache.hadoop.util.ToolRunner.run(conf, new org.apache.hadoop.fs.FsShell
				(), new string[] { "-ls", "/" }));
		}

		public class StubFileSystem : org.apache.hadoop.fs.FileSystem
		{
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
				 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
				 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
				 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.permission.AclStatus getAclStatus(org.apache.hadoop.fs.Path
				 path)
			{
				if (getConf().getBoolean("stubfs.noRpcForGetAclStatus", false))
				{
					throw new org.apache.hadoop.ipc.RemoteException(Sharpen.Runtime.getClassForType(typeof(
						org.apache.hadoop.ipc.RpcNoSuchMethodException)).getName(), "test exception");
				}
				return base.getAclStatus(path);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path
				 f)
			{
				if (f.isRoot())
				{
					return new org.apache.hadoop.fs.FileStatus(0, true, 0, 0, 0, f);
				}
				return null;
			}

			public override java.net.URI getUri()
			{
				return java.net.URI.create("stubfs:///");
			}

			public override org.apache.hadoop.fs.Path getWorkingDirectory()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
				 f)
			{
				org.apache.hadoop.fs.permission.FsPermission perm = new org.apache.hadoop.fs.permission.FsPermission
					(org.apache.hadoop.fs.permission.FsAction.ALL, org.apache.hadoop.fs.permission.FsAction
					.READ_EXECUTE, org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE);
				org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("/foo");
				org.apache.hadoop.fs.FileStatus stat = new org.apache.hadoop.fs.FileStatus(1000, 
					true, 3, 1000, 0, 0, perm, "owner", "group", path);
				return new org.apache.hadoop.fs.FileStatus[] { stat };
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool mkdirs(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.permission.FsPermission
				 permission)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
				 f, int bufferSize)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
				 dst)
			{
				return false;
			}

			public override void setWorkingDirectory(org.apache.hadoop.fs.Path dir)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		private int runCommand(string[] commands)
		{
			return org.apache.hadoop.util.ToolRunner.run(conf, new org.apache.hadoop.fs.FsShell
				(), commands);
		}
	}
}
