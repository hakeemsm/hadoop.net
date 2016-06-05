using System.Collections;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	public class TestServerConstructor : HTestCase
	{
		[Parameterized.Parameters]
		public static ICollection ConstructorFailParams()
		{
			return Arrays.AsList(new object[][] { new object[] { null, null, null, null, null
				, null }, new object[] { string.Empty, null, null, null, null, null }, new object
				[] { null, null, null, null, null, null }, new object[] { "server", null, null, 
				null, null, null }, new object[] { "server", string.Empty, null, null, null, null
				 }, new object[] { "server", "foo", null, null, null, null }, new object[] { "server"
				, "/tmp", null, null, null, null }, new object[] { "server", "/tmp", string.Empty
				, null, null, null }, new object[] { "server", "/tmp", "foo", null, null, null }
				, new object[] { "server", "/tmp", "/tmp", null, null, null }, new object[] { "server"
				, "/tmp", "/tmp", string.Empty, null, null }, new object[] { "server", "/tmp", "/tmp"
				, "foo", null, null }, new object[] { "server", "/tmp", "/tmp", "/tmp", null, null
				 }, new object[] { "server", "/tmp", "/tmp", "/tmp", string.Empty, null }, new object
				[] { "server", "/tmp", "/tmp", "/tmp", "foo", null } });
		}

		private string name;

		private string homeDir;

		private string configDir;

		private string logDir;

		private string tempDir;

		private Configuration conf;

		public TestServerConstructor(string name, string homeDir, string configDir, string
			 logDir, string tempDir, Configuration conf)
		{
			this.name = name;
			this.homeDir = homeDir;
			this.configDir = configDir;
			this.logDir = logDir;
			this.tempDir = tempDir;
			this.conf = conf;
		}

		public virtual void ConstructorFail()
		{
			new Org.Apache.Hadoop.Lib.Server.Server(name, homeDir, configDir, logDir, tempDir
				, conf);
		}
	}
}
