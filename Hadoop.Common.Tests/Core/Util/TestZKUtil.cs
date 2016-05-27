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

		[NUnit.Framework.Test]
		public virtual void TestEmptyACL()
		{
			IList<ACL> result = ZKUtil.ParseACLs(string.Empty);
			NUnit.Framework.Assert.IsTrue(result.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestNullACL()
		{
			IList<ACL> result = ZKUtil.ParseACLs(null);
			NUnit.Framework.Assert.IsTrue(result.IsEmpty());
		}

		[NUnit.Framework.Test]
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
				NUnit.Framework.Assert.AreEqual(expectedErr, e.Message);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestRemoveSpecificPerms()
		{
			int perms = ZooDefs.Perms.All;
			int remove = ZooDefs.Perms.Create;
			int newPerms = ZKUtil.RemoveSpecificPerms(perms, remove);
			NUnit.Framework.Assert.AreEqual("Removal failed", 0, newPerms & ZooDefs.Perms.Create
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestGoodACLs()
		{
			IList<ACL> result = ZKUtil.ParseACLs("sasl:hdfs/host1@MY.DOMAIN:cdrwa, sasl:hdfs/host2@MY.DOMAIN:ca"
				);
			ACL acl0 = result[0];
			NUnit.Framework.Assert.AreEqual(ZooDefs.Perms.Create | ZooDefs.Perms.Delete | ZooDefs.Perms
				.Read | ZooDefs.Perms.Write | ZooDefs.Perms.Admin, acl0.GetPerms());
			NUnit.Framework.Assert.AreEqual("sasl", acl0.GetId().GetScheme());
			NUnit.Framework.Assert.AreEqual("hdfs/host1@MY.DOMAIN", acl0.GetId().GetId());
			ACL acl1 = result[1];
			NUnit.Framework.Assert.AreEqual(ZooDefs.Perms.Create | ZooDefs.Perms.Admin, acl1.
				GetPerms());
			NUnit.Framework.Assert.AreEqual("sasl", acl1.GetId().GetScheme());
			NUnit.Framework.Assert.AreEqual("hdfs/host2@MY.DOMAIN", acl1.GetId().GetId());
		}

		[NUnit.Framework.Test]
		public virtual void TestEmptyAuth()
		{
			IList<ZKUtil.ZKAuthInfo> result = ZKUtil.ParseAuth(string.Empty);
			NUnit.Framework.Assert.IsTrue(result.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestNullAuth()
		{
			IList<ZKUtil.ZKAuthInfo> result = ZKUtil.ParseAuth(null);
			NUnit.Framework.Assert.IsTrue(result.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestGoodAuths()
		{
			IList<ZKUtil.ZKAuthInfo> result = ZKUtil.ParseAuth("scheme:data,\n   scheme2:user:pass"
				);
			NUnit.Framework.Assert.AreEqual(2, result.Count);
			ZKUtil.ZKAuthInfo auth0 = result[0];
			NUnit.Framework.Assert.AreEqual("scheme", auth0.GetScheme());
			NUnit.Framework.Assert.AreEqual("data", Sharpen.Runtime.GetStringForBytes(auth0.GetAuth
				()));
			ZKUtil.ZKAuthInfo auth1 = result[1];
			NUnit.Framework.Assert.AreEqual("scheme2", auth1.GetScheme());
			NUnit.Framework.Assert.AreEqual("user:pass", Sharpen.Runtime.GetStringForBytes(auth1
				.GetAuth()));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConfIndirection()
		{
			NUnit.Framework.Assert.IsNull(ZKUtil.ResolveConfIndirection(null));
			NUnit.Framework.Assert.AreEqual("x", ZKUtil.ResolveConfIndirection("x"));
			TestFile.GetParentFile().Mkdirs();
			Files.Write("hello world", TestFile, Charsets.Utf8);
			NUnit.Framework.Assert.AreEqual("hello world", ZKUtil.ResolveConfIndirection("@" 
				+ TestFile.GetAbsolutePath()));
			try
			{
				ZKUtil.ResolveConfIndirection("@" + BogusFile);
				NUnit.Framework.Assert.Fail("Did not throw for non-existent file reference");
			}
			catch (FileNotFoundException fnfe)
			{
				NUnit.Framework.Assert.IsTrue(fnfe.Message.StartsWith(BogusFile));
			}
		}
	}
}
