using Com.Sun.Jersey.Test.Framework;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public abstract class JerseyTestBase : JerseyTest
	{
		public JerseyTestBase(WebAppDescriptor appDescriptor)
			: base(appDescriptor)
		{
		}

		[SetUp]
		public virtual void InitializeJerseyPort()
		{
			int jerseyPort = 9998;
			string port = Runtime.GetProperty("jersey.test.port");
			if (null != port)
			{
				jerseyPort = System.Convert.ToInt32(port) + 10;
				if (jerseyPort > 65535)
				{
					jerseyPort = 9998;
				}
			}
			Runtime.SetProperty("jersey.test.port", Sharpen.Extensions.ToString(jerseyPort));
		}
	}
}
