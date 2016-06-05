using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.IO;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestZKUtil
	{
		private static readonly string TestRootDir = Runtime.GetProperty("test.build.data"
			, "/tmp") + "/TestZKUtil";

		private static readonly FilePath TestFile = new FilePath(TestRootDir, "test-file"
			);

		/// <summary>A path which is expected not to exist</summary>
		private static readonly string BogusFile = new FilePath("/xxxx-this-does-not-exist"
			).GetPath();

		[Fact]
		public virtual void TestEmptyACL()
		{
			IList<ACL> result = ZKUtil.ParseACLs(string.Empty);
			Assert.True(result.IsEmpty());
		}

		[Fact]
		public virtual void TestNullACL()
		{
			IList<ACL> result = ZKUtil.ParseACLs(null);
			Assert.True(result.IsEmpty());
		}

		[Fact]
		public virtual void TestInvalidACLs()
		{
			BadAcl("a:b", "ACL 'a:b' not of expected form scheme:id:perm");
			// not enough parts
			BadAcl("a", "ACL 'a' not of expected form scheme:id:perm");
			// not enough parts
			BadAcl("password:foo:rx", "Invalid permission 'x' in permission string 'rx'");
		}

		private static void BadAcl(string acls, string expectedErr)
		{
			try
			{
				ZKUtil.ParseACLs(acls);
				NUnit.Framework.Assert.Fail("Should have failed to parse '" + acls + "'");
			}
			catch (ZKUtil.BadAclFormatException e)
			{
				Assert.Equal(expectedErr, e.Message);
			}
		}

		[Fact]
		public virtual void TestRemoveSpecificPerms()
		{
			int perms = ZooDefs.Perms.All;
			int remove = ZooDefs.Perms.Create;
			int newPerms = ZKUtil.RemoveSpecificPerms(perms, remove);
			Assert.Equal("Removal failed", 0, newPerms & ZooDefs.Perms.Create
				);
		}

		[Fact]
		public virtual void TestGoodACLs()
		{
			IList<ACL> result = ZKUtil.ParseACLs("sasl:hdfs/host1@MY.DOMAIN:cdrwa, sasl:hdfs/host2@MY.DOMAIN:ca"
				);
			ACL acl0 = result[0];
			Assert.Equal(ZooDefs.Perms.Create | ZooDefs.Perms.Delete | ZooDefs.Perms
				.Read | ZooDefs.Perms.Write | ZooDefs.Perms.Admin, acl0.GetPerms());
			Assert.Equal("sasl", acl0.GetId().GetScheme());
			Assert.Equal("hdfs/host1@MY.DOMAIN", acl0.GetId().GetId());
			ACL acl1 = result[1];
			Assert.Equal(ZooDefs.Perms.Create | ZooDefs.Perms.Admin, acl1.
				GetPerms());
			Assert.Equal("sasl", acl1.GetId().GetScheme());
			Assert.Equal("hdfs/host2@MY.DOMAIN", acl1.GetId().GetId());
		}

		[Fact]
		public virtual void TestEmptyAuth()
		{
			IList<ZKUtil.ZKAuthInfo> result = ZKUtil.ParseAuth(string.Empty);
			Assert.True(result.IsEmpty());
		}

		[Fact]
		public virtual void TestNullAuth()
		{
			IList<ZKUtil.ZKAuthInfo> result = ZKUtil.ParseAuth(null);
			Assert.True(result.IsEmpty());
		}

		[Fact]
		public virtual void TestGoodAuths()
		{
			IList<ZKUtil.ZKAuthInfo> result = ZKUtil.ParseAuth("scheme:data,\n   scheme2:user:pass"
				);
			Assert.Equal(2, result.Count);
			ZKUtil.ZKAuthInfo auth0 = result[0];
			Assert.Equal("scheme", auth0.GetScheme());
			Assert.Equal("data", Sharpen.Runtime.GetStringForBytes(auth0.GetAuth
				()));
			ZKUtil.ZKAuthInfo auth1 = result[1];
			Assert.Equal("scheme2", auth1.GetScheme());
			Assert.Equal("user:pass", Sharpen.Runtime.GetStringForBytes(auth1
				.GetAuth()));
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestConfIndirection()
		{
			NUnit.Framework.Assert.IsNull(ZKUtil.ResolveConfIndirection(null));
			Assert.Equal("x", ZKUtil.ResolveConfIndirection("x"));
			TestFile.GetParentFile().Mkdirs();
			Files.Write("hello world", TestFile, Charsets.Utf8);
			Assert.Equal("hello world", ZKUtil.ResolveConfIndirection("@" 
				+ TestFile.GetAbsolutePath()));
			try
			{
				ZKUtil.ResolveConfIndirection("@" + BogusFile);
				NUnit.Framework.Assert.Fail("Did not throw for non-existent file reference");
			}
			catch (FileNotFoundException fnfe)
			{
				Assert.True(fnfe.Message.StartsWith(BogusFile));
			}
		}
	}
}
