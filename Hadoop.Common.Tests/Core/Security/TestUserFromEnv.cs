using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class TestUserFromEnv
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUserFromEnvironment()
		{
			Runtime.SetProperty(UserGroupInformation.HadoopUserName, "randomUser");
			NUnit.Framework.Assert.AreEqual("randomUser", UserGroupInformation.GetLoginUser()
				.GetUserName());
		}
	}
}
