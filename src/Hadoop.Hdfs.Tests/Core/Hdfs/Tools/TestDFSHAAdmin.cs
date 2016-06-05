using System;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	public class TestDFSHAAdmin
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDFSHAAdmin));

		private DFSHAAdmin tool;

		private readonly ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();

		private readonly ByteArrayOutputStream outBytes = new ByteArrayOutputStream();

		private string errOutput;

		private string output;

		private HAServiceProtocol mockProtocol;

		private ZKFCProtocol mockZkfcProtocol;

		private const string Nsid = "ns1";

		private static readonly HAServiceStatus StandbyReadyResult = new HAServiceStatus(
			HAServiceProtocol.HAServiceState.Standby).SetReadyToBecomeActive();

		private readonly ArgumentCaptor<HAServiceProtocol.StateChangeRequestInfo> reqInfoCaptor
			 = ArgumentCaptor.ForClass<HAServiceProtocol.StateChangeRequestInfo>();

		private const string HostA = "1.2.3.1";

		private const string HostB = "1.2.3.2";

		private const string FencerTrueCommandUnix = "shell(true)";

		private const string FencerFalseCommandUnix = "shell(false)";

		private const string FencerTrueCommandWindows = "shell(rem)";

		private const string FencerFalseCommandWindows = "shell(help.exe /? >NUL)";

		// Fencer shell commands that always return true and false respectively
		// on Unix.
		// Fencer shell commands that always return true and false respectively
		// on Windows. Lacking POSIX 'true' and 'false' commands we use the DOS
		// commands 'rem' and 'help.exe'.
		private HdfsConfiguration GetHAConf()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsNameservices, Nsid);
			conf.Set(DFSConfigKeys.DfsNameserviceId, Nsid);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, Nsid), "nn1,nn2"
				);
			conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, "nn1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nsid, "nn1"
				), HostA + ":12345");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nsid, "nn2"
				), HostB + ":12345");
			return conf;
		}

		public static string GetFencerTrueCommand()
		{
			return Shell.Windows ? FencerTrueCommandWindows : FencerTrueCommandUnix;
		}

		public static string GetFencerFalseCommand()
		{
			return Shell.Windows ? FencerFalseCommandWindows : FencerFalseCommandUnix;
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			mockProtocol = MockitoUtil.MockProtocol<HAServiceProtocol>();
			mockZkfcProtocol = MockitoUtil.MockProtocol<ZKFCProtocol>();
			tool = new _DFSHAAdmin_118(this);
			// OVerride the target to return our mock protocol
			// mock setup doesn't really throw
			tool.SetConf(GetHAConf());
			tool.SetErrOut(new TextWriter(errOutBytes));
			tool.SetOut(new TextWriter(outBytes));
		}

		private sealed class _DFSHAAdmin_118 : DFSHAAdmin
		{
			public _DFSHAAdmin_118(TestDFSHAAdmin _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override HAServiceTarget ResolveTarget(string nnId)
			{
				HAServiceTarget target = base.ResolveTarget(nnId);
				HAServiceTarget spy = Org.Mockito.Mockito.Spy(target);
				try
				{
					Org.Mockito.Mockito.DoReturn(this._enclosing.mockProtocol).When(spy).GetProxy(Org.Mockito.Mockito
						.Any<Configuration>(), Org.Mockito.Mockito.AnyInt());
					Org.Mockito.Mockito.DoReturn(this._enclosing.mockZkfcProtocol).When(spy).GetZKFCProxy
						(Org.Mockito.Mockito.Any<Configuration>(), Org.Mockito.Mockito.AnyInt());
				}
				catch (IOException e)
				{
					throw new Exception(e);
				}
				return spy;
			}

			private readonly TestDFSHAAdmin _enclosing;
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
		public virtual void TestNameserviceOption()
		{
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-ns"));
			AssertOutputContains("Missing nameservice ID");
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-ns", "ns1"));
			AssertOutputContains("Missing command");
			// "ns1" isn't defined but we check this lazily and help doesn't use the ns
			NUnit.Framework.Assert.AreEqual(0, RunTool("-ns", "ns1", "-help", "transitionToActive"
				));
			AssertOutputContains("Transitions the service into Active");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNamenodeResolution()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).GetServiceStatus();
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-getServiceState", "undefined"));
			AssertOutputContains("Unable to determine service address for namenode 'undefined'"
				);
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
		[NUnit.Framework.Test]
		public virtual void TestTransitionToActive()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToActive", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).TransitionToActive(reqInfoCaptor.Capture
				());
			NUnit.Framework.Assert.AreEqual(HAServiceProtocol.RequestSource.RequestByUser, reqInfoCaptor
				.GetValue().GetSource());
		}

		/// <summary>
		/// Test that, if automatic HA is enabled, none of the mutative operations
		/// will succeed, unless the -forcemanual flag is specified.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMutativeOperationsWithAutoHaEnabled()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			// Turn on auto-HA in the config
			HdfsConfiguration conf = GetHAConf();
			conf.SetBoolean(DFSConfigKeys.DfsHaAutoFailoverEnabledKey, true);
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			// Should fail without the forcemanual flag
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-transitionToActive", "nn1"));
			NUnit.Framework.Assert.IsTrue(errOutput.Contains("Refusing to manually manage"));
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-transitionToStandby", "nn1"));
			NUnit.Framework.Assert.IsTrue(errOutput.Contains("Refusing to manually manage"));
			Org.Mockito.Mockito.Verify(mockProtocol, Org.Mockito.Mockito.Never()).TransitionToActive
				(AnyReqInfo());
			Org.Mockito.Mockito.Verify(mockProtocol, Org.Mockito.Mockito.Never()).TransitionToStandby
				(AnyReqInfo());
			// Force flag should bypass the check and change the request source
			// for the RPC
			SetupConfirmationOnSystemIn();
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToActive", "-forcemanual", 
				"nn1"));
			SetupConfirmationOnSystemIn();
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToStandby", "-forcemanual"
				, "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol, Org.Mockito.Mockito.Times(1)).TransitionToActive
				(reqInfoCaptor.Capture());
			Org.Mockito.Mockito.Verify(mockProtocol, Org.Mockito.Mockito.Times(1)).TransitionToStandby
				(reqInfoCaptor.Capture());
			// All of the RPCs should have had the "force" source
			foreach (HAServiceProtocol.StateChangeRequestInfo ri in reqInfoCaptor.GetAllValues
				())
			{
				NUnit.Framework.Assert.AreEqual(HAServiceProtocol.RequestSource.RequestByUserForced
					, ri.GetSource());
			}
		}

		/// <summary>
		/// Setup System.in with a stream that feeds a "yes" answer on the
		/// next prompt.
		/// </summary>
		private static void SetupConfirmationOnSystemIn()
		{
			// Answer "yes" to the prompt about transition to active
			Runtime.SetIn(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString("yes\n")
				));
		}

		/// <summary>
		/// Test that, even if automatic HA is enabled, the monitoring operations
		/// still function correctly.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMonitoringOperationsWithAutoHaEnabled()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			// Turn on auto-HA
			HdfsConfiguration conf = GetHAConf();
			conf.SetBoolean(DFSConfigKeys.DfsHaAutoFailoverEnabledKey, true);
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-checkHealth", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).MonitorHealth();
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).GetServiceStatus();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTransitionToStandby()
		{
			NUnit.Framework.Assert.AreEqual(0, RunTool("-transitionToStandby", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).TransitionToStandby(AnyReqInfo());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithNoFencerConfigured()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithFencerConfigured()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithFencerAndNameservice()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-ns", "ns1", "-failover", "nn1", "nn2"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithFencerConfiguredAndForce()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithForceActive()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2", "--forceactive"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithInvalidFenceArg()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "notforcefence"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithFenceButNoFencer()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithFenceAndBadFencer()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, "foobar!");
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailoverWithAutoHa()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			// Turn on auto-HA in the config
			HdfsConfiguration conf = GetHAConf();
			conf.SetBoolean(DFSConfigKeys.DfsHaAutoFailoverEnabledKey, true);
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2"));
			Org.Mockito.Mockito.Verify(mockZkfcProtocol).GracefulFailover();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestForceFenceOptionListedBeforeArgs()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			HdfsConfiguration conf = GetHAConf();
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "--forcefence", "nn1", "nn2"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetServiceStatus()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			NUnit.Framework.Assert.AreEqual(0, RunTool("-getServiceState", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).GetServiceStatus();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckHealth()
		{
			NUnit.Framework.Assert.AreEqual(0, RunTool("-checkHealth", "nn1"));
			Org.Mockito.Mockito.Verify(mockProtocol).MonitorHealth();
			Org.Mockito.Mockito.DoThrow(new HealthCheckFailedException("fake health check failure"
				)).When(mockProtocol).MonitorHealth();
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-checkHealth", "nn1"));
			AssertOutputContains("Health check failed: fake health check failure");
		}

		/// <summary>
		/// Test that the fencing configuration can be overridden per-nameservice
		/// or per-namenode
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFencingConfigPerNameNode()
		{
			Org.Mockito.Mockito.DoReturn(StandbyReadyResult).When(mockProtocol).GetServiceStatus
				();
			string nsSpecificKey = DFSConfigKeys.DfsHaFenceMethodsKey + "." + Nsid;
			string nnSpecificKey = nsSpecificKey + ".nn1";
			HdfsConfiguration conf = GetHAConf();
			// Set the default fencer to succeed
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
			// Set the NN-specific fencer to fail. Should fail to fence.
			conf.Set(nnSpecificKey, GetFencerFalseCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
			conf.Unset(nnSpecificKey);
			// Set an NS-specific fencer to fail. Should fail.
			conf.Set(nsSpecificKey, GetFencerFalseCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(-1, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
			// Set the NS-specific fencer to succeed. Should succeed
			conf.Set(nsSpecificKey, GetFencerTrueCommand());
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, RunTool("-failover", "nn1", "nn2", "--forcefence"
				));
		}

		/// <exception cref="System.Exception"/>
		private object RunTool(params string[] args)
		{
			errOutBytes.Reset();
			outBytes.Reset();
			Log.Info("Running: DFSHAAdmin " + Joiner.On(" ").Join(args));
			int ret = tool.Run(args);
			errOutput = new string(errOutBytes.ToByteArray(), Charsets.Utf8);
			output = new string(outBytes.ToByteArray(), Charsets.Utf8);
			Log.Info("Err_output:\n" + errOutput + "\nOutput:\n" + output);
			return ret;
		}

		private HAServiceProtocol.StateChangeRequestInfo AnyReqInfo()
		{
			return Org.Mockito.Mockito.Any();
		}
	}
}
