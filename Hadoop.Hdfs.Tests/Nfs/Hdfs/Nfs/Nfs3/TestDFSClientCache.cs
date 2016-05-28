using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Security;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestDFSClientCache
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEviction()
		{
			NfsConfiguration conf = new NfsConfiguration();
			conf.Set(FileSystem.FsDefaultNameKey, "hdfs://localhost");
			// Only one entry will be in the cache
			int MaxCacheSize = 1;
			DFSClientCache cache = new DFSClientCache(conf, MaxCacheSize);
			DFSClient c1 = cache.GetDfsClient("test1");
			NUnit.Framework.Assert.IsTrue(cache.GetDfsClient("test1").ToString().Contains("ugi=test1"
				));
			NUnit.Framework.Assert.AreEqual(c1, cache.GetDfsClient("test1"));
			NUnit.Framework.Assert.IsFalse(IsDfsClientClose(c1));
			cache.GetDfsClient("test2");
			NUnit.Framework.Assert.IsTrue(IsDfsClientClose(c1));
			NUnit.Framework.Assert.IsTrue("cache size should be the max size or less", cache.
				clientCache.Size() <= MaxCacheSize);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetUserGroupInformationSecure()
		{
			string userName = "user1";
			string currentUser = "test-user";
			NfsConfiguration conf = new NfsConfiguration();
			UserGroupInformation currentUserUgi = UserGroupInformation.CreateRemoteUser(currentUser
				);
			currentUserUgi.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.
				Kerberos);
			UserGroupInformation.SetLoginUser(currentUserUgi);
			DFSClientCache cache = new DFSClientCache(conf);
			UserGroupInformation ugiResult = cache.GetUserGroupInformation(userName, currentUserUgi
				);
			Assert.AssertThat(ugiResult.GetUserName(), IS.Is(userName));
			Assert.AssertThat(ugiResult.GetRealUser(), IS.Is(currentUserUgi));
			Assert.AssertThat(ugiResult.GetAuthenticationMethod(), IS.Is(UserGroupInformation.AuthenticationMethod
				.Proxy));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetUserGroupInformation()
		{
			string userName = "user1";
			string currentUser = "currentUser";
			UserGroupInformation currentUserUgi = UserGroupInformation.CreateUserForTesting(currentUser
				, new string[0]);
			NfsConfiguration conf = new NfsConfiguration();
			conf.Set(FileSystem.FsDefaultNameKey, "hdfs://localhost");
			DFSClientCache cache = new DFSClientCache(conf);
			UserGroupInformation ugiResult = cache.GetUserGroupInformation(userName, currentUserUgi
				);
			Assert.AssertThat(ugiResult.GetUserName(), IS.Is(userName));
			Assert.AssertThat(ugiResult.GetRealUser(), IS.Is(currentUserUgi));
			Assert.AssertThat(ugiResult.GetAuthenticationMethod(), IS.Is(UserGroupInformation.AuthenticationMethod
				.Proxy));
		}

		private static bool IsDfsClientClose(DFSClient c)
		{
			try
			{
				c.Exists(string.Empty);
			}
			catch (IOException e)
			{
				return e.Message.Equals("Filesystem closed");
			}
			return false;
		}
	}
}
