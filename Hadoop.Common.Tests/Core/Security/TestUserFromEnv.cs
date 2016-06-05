using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestUserFromEnv
	{
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestUserFromEnvironment()
		{
			Runtime.SetProperty(UserGroupInformation.HadoopUserName, "randomUser");
			Assert.Equal("randomUser", UserGroupInformation.GetLoginUser()
				.GetUserName());
		}
	}
}
