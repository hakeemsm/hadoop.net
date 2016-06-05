using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	public class TestParam
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestParam));

		internal readonly Configuration conf = new Configuration();

		[NUnit.Framework.Test]
		public virtual void TestAccessTimeParam()
		{
			AccessTimeParam p = new AccessTimeParam(AccessTimeParam.Default);
			NUnit.Framework.Assert.AreEqual(-1L, p.GetValue());
			new AccessTimeParam(-1L);
			try
			{
				new AccessTimeParam(-2L);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestBlockSizeParam()
		{
			BlockSizeParam p = new BlockSizeParam(BlockSizeParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
			NUnit.Framework.Assert.AreEqual(conf.GetLongBytes(DFSConfigKeys.DfsBlockSizeKey, 
				DFSConfigKeys.DfsBlockSizeDefault), p.GetValue(conf));
			new BlockSizeParam(1L);
			try
			{
				new BlockSizeParam(0L);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestBufferSizeParam()
		{
			BufferSizeParam p = new BufferSizeParam(BufferSizeParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
			NUnit.Framework.Assert.AreEqual(conf.GetInt(CommonConfigurationKeysPublic.IoFileBufferSizeKey
				, CommonConfigurationKeysPublic.IoFileBufferSizeDefault), p.GetValue(conf));
			new BufferSizeParam(1);
			try
			{
				new BufferSizeParam(0);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestDelegationParam()
		{
			DelegationParam p = new DelegationParam(DelegationParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void TestDestinationParam()
		{
			DestinationParam p = new DestinationParam(DestinationParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
			new DestinationParam("/abc");
			try
			{
				new DestinationParam("abc");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGroupParam()
		{
			GroupParam p = new GroupParam(GroupParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void TestModificationTimeParam()
		{
			ModificationTimeParam p = new ModificationTimeParam(ModificationTimeParam.Default
				);
			NUnit.Framework.Assert.AreEqual(-1L, p.GetValue());
			new ModificationTimeParam(-1L);
			try
			{
				new ModificationTimeParam(-2L);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestOverwriteParam()
		{
			OverwriteParam p = new OverwriteParam(OverwriteParam.Default);
			NUnit.Framework.Assert.AreEqual(false, p.GetValue());
			new OverwriteParam("trUe");
			try
			{
				new OverwriteParam("abc");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestOwnerParam()
		{
			OwnerParam p = new OwnerParam(OwnerParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void TestPermissionParam()
		{
			PermissionParam p = new PermissionParam(PermissionParam.Default);
			NUnit.Framework.Assert.AreEqual(new FsPermission((short)0x1ed), p.GetFsPermission
				());
			new PermissionParam("0");
			try
			{
				new PermissionParam("-1");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
			new PermissionParam("1777");
			try
			{
				new PermissionParam("2000");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
			try
			{
				new PermissionParam("8");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
			try
			{
				new PermissionParam("abc");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestRecursiveParam()
		{
			RecursiveParam p = new RecursiveParam(RecursiveParam.Default);
			NUnit.Framework.Assert.AreEqual(false, p.GetValue());
			new RecursiveParam("falSe");
			try
			{
				new RecursiveParam("abc");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestRenewerParam()
		{
			RenewerParam p = new RenewerParam(RenewerParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void TestReplicationParam()
		{
			ReplicationParam p = new ReplicationParam(ReplicationParam.Default);
			NUnit.Framework.Assert.AreEqual(null, p.GetValue());
			NUnit.Framework.Assert.AreEqual((short)conf.GetInt(DFSConfigKeys.DfsReplicationKey
				, DFSConfigKeys.DfsReplicationDefault), p.GetValue(conf));
			new ReplicationParam((short)1);
			try
			{
				new ReplicationParam((short)0);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestToSortedStringEscapesURICharacters()
		{
			string sep = "&";
			Param<object, object> ampParam = new TokenArgumentParam("token&ampersand");
			Param<object, object> equalParam = new RenewerParam("renewer=equal");
			string expected = "&renewer=renewer%3Dequal&token=token%26ampersand";
			string actual = Param.ToSortedString(sep, equalParam, ampParam);
			NUnit.Framework.Assert.AreEqual(expected, actual);
		}

		[NUnit.Framework.Test]
		public virtual void UserNameEmpty()
		{
			UserParam userParam = new UserParam(string.Empty);
			NUnit.Framework.Assert.IsNull(userParam.GetValue());
		}

		public virtual void UserNameInvalidStart()
		{
			new UserParam("1x");
		}

		public virtual void UserNameInvalidDollarSign()
		{
			new UserParam("1$x");
		}

		[NUnit.Framework.Test]
		public virtual void UserNameMinLength()
		{
			UserParam userParam = new UserParam("a");
			NUnit.Framework.Assert.IsNotNull(userParam.GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void UserNameValidDollarSign()
		{
			UserParam userParam = new UserParam("a$");
			NUnit.Framework.Assert.IsNotNull(userParam.GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void TestConcatSourcesParam()
		{
			string[] strings = new string[] { "/", "/foo", "/bar" };
			for (int n = 0; n < strings.Length; n++)
			{
				string[] sub = new string[n];
				Path[] paths = new Path[n];
				for (int i = 0; i < paths.Length; i++)
				{
					paths[i] = new Path(sub[i] = strings[i]);
				}
				string expected = StringUtils.Join(",", Arrays.AsList(sub));
				ConcatSourcesParam computed = new ConcatSourcesParam(paths);
				NUnit.Framework.Assert.AreEqual(expected, computed.GetValue());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestUserNameOkAfterResettingPattern()
		{
			StringParam.Domain oldDomain = UserParam.GetUserPatternDomain();
			string newPattern = "^[A-Za-z0-9_][A-Za-z0-9._-]*[$]?$";
			UserParam.SetUserPattern(newPattern);
			UserParam userParam = new UserParam("1x");
			NUnit.Framework.Assert.IsNotNull(userParam.GetValue());
			userParam = new UserParam("123");
			NUnit.Framework.Assert.IsNotNull(userParam.GetValue());
			UserParam.SetUserPatternDomain(oldDomain);
		}

		[NUnit.Framework.Test]
		public virtual void TestAclPermissionParam()
		{
			AclPermissionParam p = new AclPermissionParam("user::rwx,group::r--,other::rwx,user:user1:rwx"
				);
			IList<AclEntry> setAclList = AclEntry.ParseAclSpec("user::rwx,group::r--,other::rwx,user:user1:rwx"
				, true);
			NUnit.Framework.Assert.AreEqual(setAclList.ToString(), p.GetAclPermission(true).ToString
				());
			new AclPermissionParam("user::rw-,group::rwx,other::rw-,user:user1:rwx");
			try
			{
				new AclPermissionParam("user::rw--,group::rwx-,other::rw-");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
			new AclPermissionParam("user::rw-,group::rwx,other::rw-,user:user1:rwx,group:group1:rwx,other::rwx,mask::rwx,default:user:user1:rwx"
				);
			try
			{
				new AclPermissionParam("user:r-,group:rwx,other:rw-");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
			try
			{
				new AclPermissionParam("default:::r-,default:group::rwx,other::rw-");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
			try
			{
				new AclPermissionParam("user:r-,group::rwx,other:rw-,mask:rw-,temp::rwx");
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				Log.Info("EXPECTED: " + e);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestXAttrNameParam()
		{
			XAttrNameParam p = new XAttrNameParam("user.a1");
			NUnit.Framework.Assert.AreEqual(p.GetXAttrName(), "user.a1");
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestXAttrValueParam()
		{
			XAttrValueParam p = new XAttrValueParam("0x313233");
			Assert.AssertArrayEquals(p.GetXAttrValue(), XAttrCodec.DecodeValue("0x313233"));
		}

		[NUnit.Framework.Test]
		public virtual void TestXAttrEncodingParam()
		{
			XAttrEncodingParam p = new XAttrEncodingParam(XAttrCodec.Base64);
			NUnit.Framework.Assert.AreEqual(p.GetEncoding(), XAttrCodec.Base64);
			XAttrEncodingParam p1 = new XAttrEncodingParam(p.GetValueString());
			NUnit.Framework.Assert.AreEqual(p1.GetEncoding(), XAttrCodec.Base64);
		}

		[NUnit.Framework.Test]
		public virtual void TestXAttrSetFlagParam()
		{
			EnumSet<XAttrSetFlag> flag = EnumSet.Of(XAttrSetFlag.Create, XAttrSetFlag.Replace
				);
			XAttrSetFlagParam p = new XAttrSetFlagParam(flag);
			NUnit.Framework.Assert.AreEqual(p.GetFlag(), flag);
			XAttrSetFlagParam p1 = new XAttrSetFlagParam(p.GetValueString());
			NUnit.Framework.Assert.AreEqual(p1.GetFlag(), flag);
		}

		[NUnit.Framework.Test]
		public virtual void TestRenameOptionSetParam()
		{
			RenameOptionSetParam p = new RenameOptionSetParam(Options.Rename.Overwrite, Options.Rename
				.None);
			RenameOptionSetParam p1 = new RenameOptionSetParam(p.GetValueString());
			NUnit.Framework.Assert.AreEqual(p1.GetValue(), EnumSet.Of(Options.Rename.Overwrite
				, Options.Rename.None));
		}

		[NUnit.Framework.Test]
		public virtual void TestSnapshotNameParam()
		{
			OldSnapshotNameParam s1 = new OldSnapshotNameParam("s1");
			SnapshotNameParam s2 = new SnapshotNameParam("s2");
			NUnit.Framework.Assert.AreEqual("s1", s1.GetValue());
			NUnit.Framework.Assert.AreEqual("s2", s2.GetValue());
		}
	}
}
