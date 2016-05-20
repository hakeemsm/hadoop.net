using Sharpen;

namespace org.apache.hadoop.ha
{
	public class TestHAAdmin
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.TestHAAdmin)
			));

		private org.apache.hadoop.ha.HAAdmin tool;

		private java.io.ByteArrayOutputStream errOutBytes = new java.io.ByteArrayOutputStream
			();

		private java.io.ByteArrayOutputStream outBytes = new java.io.ByteArrayOutputStream
			();

		private string errOutput;

		private string output;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			tool = new _HAAdmin_49();
			tool.setConf(new org.apache.hadoop.conf.Configuration());
			tool.errOut = new System.IO.TextWriter(errOutBytes);
			tool.@out = new System.IO.TextWriter(outBytes);
		}

		private sealed class _HAAdmin_49 : org.apache.hadoop.ha.HAAdmin
		{
			public _HAAdmin_49()
			{
			}

			protected internal override org.apache.hadoop.ha.HAServiceTarget resolveTarget(string
				 target)
			{
				return new org.apache.hadoop.ha.DummyHAService(org.apache.hadoop.ha.HAServiceProtocol.HAServiceState
					.STANDBY, new java.net.InetSocketAddress("dummy", 12345));
			}
		}

		private void assertOutputContains(string @string)
		{
			if (!errOutput.contains(@string) && !output.contains(@string))
			{
				NUnit.Framework.Assert.Fail("Expected output to contain '" + @string + "' but err_output was:\n"
					 + errOutput + "\n and output was: \n" + output);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAdminUsage()
		{
			NUnit.Framework.Assert.AreEqual(-1, runTool());
			assertOutputContains("Usage:");
			assertOutputContains("-transitionToActive");
			NUnit.Framework.Assert.AreEqual(-1, runTool("badCommand"));
			assertOutputContains("Bad command 'badCommand'");
			NUnit.Framework.Assert.AreEqual(-1, runTool("-badCommand"));
			assertOutputContains("badCommand: Unknown");
			// valid command but not enough arguments
			NUnit.Framework.Assert.AreEqual(-1, runTool("-transitionToActive"));
			assertOutputContains("transitionToActive: incorrect number of arguments");
			NUnit.Framework.Assert.AreEqual(-1, runTool("-transitionToActive", "x", "y"));
			assertOutputContains("transitionToActive: incorrect number of arguments");
			NUnit.Framework.Assert.AreEqual(-1, runTool("-failover"));
			assertOutputContains("failover: incorrect arguments");
			assertOutputContains("failover: incorrect arguments");
			NUnit.Framework.Assert.AreEqual(-1, runTool("-failover", "foo:1234"));
			assertOutputContains("failover: incorrect arguments");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testHelp()
		{
			NUnit.Framework.Assert.AreEqual(0, runTool("-help"));
			NUnit.Framework.Assert.AreEqual(0, runTool("-help", "transitionToActive"));
			assertOutputContains("Transitions the service into Active");
		}

		/// <exception cref="System.Exception"/>
		private object runTool(params string[] args)
		{
			errOutBytes.reset();
			outBytes.reset();
			LOG.info("Running: HAAdmin " + com.google.common.@base.Joiner.on(" ").join(args));
			int ret = tool.run(args);
			errOutput = new string(errOutBytes.toByteArray(), com.google.common.@base.Charsets
				.UTF_8);
			output = new string(outBytes.toByteArray(), com.google.common.@base.Charsets.UTF_8
				);
			LOG.info("Err_output:\n" + errOutput + "\nOutput:\n" + output);
			return ret;
		}
	}
}
