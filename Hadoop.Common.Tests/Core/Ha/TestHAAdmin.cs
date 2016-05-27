using System.IO;
using System.Net;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.HA
{
	public class TestHAAdmin
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestHAAdmin));

		private HAAdmin tool;

		private ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();

		private ByteArrayOutputStream outBytes = new ByteArrayOutputStream();

		private string errOutput;

		private string output;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			tool = new _HAAdmin_49();
			tool.SetConf(new Configuration());
			tool.errOut = new TextWriter(errOutBytes);
			tool.@out = new TextWriter(outBytes);
		}

		private sealed class _HAAdmin_49 : HAAdmin
		{
			public _HAAdmin_49()
			{
			}

			protected internal override HAServiceTarget ResolveTarget(string target)
			{
				return new DummyHAService(HAServiceProtocol.HAServiceState.Standby, new IPEndPoint
					("dummy", 12345));
			}
		}

		private void AssertOutputContains(string @string)
		{
			if (!errOutput.Contains(@string) && !output.Contains(@string))
			{
				NUnit.Framework.Assert.Fail("Expected output to contain '" + @string + "' but err_output was:\n"
					 + errOutput + "\n and output was: \n" + output);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAdminUsage()
		{
			NUnit.Framework.Assert.AreEqual(-1, RunTool());
			AssertOutputContains("Usage:");
			AssertOutputContains("-transitionToActive");
			NUnit.Framework.Assert.AreEqual(-1, RunTool("badCommand"));
			AssertOutputContains("Bad command 'badCommand'");
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-badCommand"));
			AssertOutputContains("badCommand: Unknown");
			// valid command but not enough arguments
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-transitionToActive"));
			AssertOutputContains("transitionToActive: incorrect number of arguments");
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-transitionToActive", "x", "y"));
			AssertOutputContains("transitionToActive: incorrect number of arguments");
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover"));
			AssertOutputContains("failover: incorrect arguments");
			AssertOutputContains("failover: incorrect arguments");
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "foo:1234"));
			AssertOutputContains("failover: incorrect arguments");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHelp()
		{
			NUnit.Framework.Assert.AreEqual(0, RunTool("-help"));
			NUnit.Framework.Assert.AreEqual(0, RunTool("-help", "transitionToActive"));
			AssertOutputContains("Transitions the service into Active");
		}

		/// <exception cref="System.Exception"/>
		private object RunTool(params string[] args)
		{
			errOutBytes.Reset();
			outBytes.Reset();
			Log.Info("Running: HAAdmin " + Joiner.On(" ").Join(args));
			int ret = tool.Run(args);
			errOutput = new string(errOutBytes.ToByteArray(), Charsets.Utf8);
			output = new string(outBytes.ToByteArray(), Charsets.Utf8);
			Log.Info("Err_output:\n" + errOutput + "\nOutput:\n" + output);
			return ret;
		}
	}
}
