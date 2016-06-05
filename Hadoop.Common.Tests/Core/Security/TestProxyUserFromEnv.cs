using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestProxyUserFromEnv
	{
		/// <summary>Test HADOOP_PROXY_USER for impersonation</summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestProxyUserFromEnvironment()
		{
			string proxyUser = "foo.bar";
			Runtime.SetProperty(UserGroupInformation.HadoopProxyUser, proxyUser);
			UserGroupInformation ugi = UserGroupInformation.GetLoginUser();
			Assert.Equal(proxyUser, ugi.GetUserName());
			UserGroupInformation realUgi = ugi.GetRealUser();
			NUnit.Framework.Assert.IsNotNull(realUgi);
			// get the expected real user name
			SystemProcess pp = Runtime.GetRuntime().Exec("whoami");
			BufferedReader br = new BufferedReader(new InputStreamReader(pp.GetInputStream())
				);
			string realUser = br.ReadLine().Trim();
			// On Windows domain joined machine, whoami returns the username
			// in the DOMAIN\\username format, so we trim the domain part before
			// the comparison. We don't have to special case for Windows
			// given that Unix systems do not allow slashes in usernames.
			int backslashIndex = realUser.IndexOf('\\');
			if (backslashIndex != -1)
			{
				realUser = Sharpen.Runtime.Substring(realUser, backslashIndex + 1);
			}
			Assert.Equal(realUser, realUgi.GetUserName());
		}
	}
}
