using NUnit.Framework;


namespace Org.Apache.Hadoop.Minikdc
{
	/// <summary>
	/// KerberosSecurityTestcase provides a base class for using MiniKdc with other
	/// testcases.
	/// </summary>
	/// <remarks>
	/// KerberosSecurityTestcase provides a base class for using MiniKdc with other
	/// testcases. KerberosSecurityTestcase starts the MiniKdc (@Before) before
	/// running tests, and stop the MiniKdc (@After) after the testcases, using
	/// default settings (working dir and kdc configurations).
	/// <p>
	/// Users can directly inherit this class and implement their own test functions
	/// using the default settings, or override functions getTestDir() and
	/// createMiniKdcConf() to provide new settings.
	/// </remarks>
	public class KerberosSecurityTestcase
	{
		private MiniKdc kdc;

		private FilePath workDir;

		private Properties conf;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void StartMiniKdc()
		{
			CreateTestDir();
			CreateMiniKdcConf();
			kdc = new MiniKdc(conf, workDir);
			kdc.Start();
		}

		/// <summary>Create a working directory, it should be the build directory.</summary>
		/// <remarks>
		/// Create a working directory, it should be the build directory. Under
		/// this directory an ApacheDS working directory will be created, this
		/// directory will be deleted when the MiniKdc stops.
		/// </remarks>
		public virtual void CreateTestDir()
		{
			workDir = new FilePath(Runtime.GetProperty("test.dir", "target"));
		}

		/// <summary>Create a Kdc configuration</summary>
		public virtual void CreateMiniKdcConf()
		{
			conf = MiniKdc.CreateConf();
		}

		[TearDown]
		public virtual void StopMiniKdc()
		{
			if (kdc != null)
			{
				kdc.Stop();
			}
		}

		public virtual MiniKdc GetKdc()
		{
			return kdc;
		}

		public virtual FilePath GetWorkDir()
		{
			return workDir;
		}

		public virtual Properties GetConf()
		{
			return conf;
		}
	}
}
