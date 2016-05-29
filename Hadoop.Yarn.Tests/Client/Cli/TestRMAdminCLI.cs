using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Cli
{
	public class TestRMAdminCLI
	{
		private ResourceManagerAdministrationProtocol admin;

		private HAServiceProtocol haadmin;

		private RMAdminCLI rmAdminCLI;

		private RMAdminCLI rmAdminCLIWithHAEnabled;

		private CommonNodeLabelsManager dummyNodeLabelsManager;

		private bool remoteAdminServiceAccessed = false;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[SetUp]
		public virtual void Configure()
		{
			remoteAdminServiceAccessed = false;
			admin = Org.Mockito.Mockito.Mock<ResourceManagerAdministrationProtocol>();
			Org.Mockito.Mockito.When(admin.AddToClusterNodeLabels(Matchers.Any<AddToClusterNodeLabelsRequest
				>())).ThenAnswer(new _Answer_79(this));
			haadmin = Org.Mockito.Mockito.Mock<HAServiceProtocol>();
			Org.Mockito.Mockito.When(haadmin.GetServiceStatus()).ThenReturn(new HAServiceStatus
				(HAServiceProtocol.HAServiceState.Initializing));
			HAServiceTarget haServiceTarget = Org.Mockito.Mockito.Mock<HAServiceTarget>();
			Org.Mockito.Mockito.When(haServiceTarget.GetProxy(Matchers.Any<Configuration>(), 
				Matchers.AnyInt())).ThenReturn(haadmin);
			rmAdminCLI = new _RMAdminCLI_96(this, haServiceTarget, new Configuration());
			InitDummyNodeLabelsManager();
			rmAdminCLI.localNodeLabelsManager = dummyNodeLabelsManager;
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			rmAdminCLIWithHAEnabled = new _RMAdminCLI_113(this, haServiceTarget, conf);
		}

		private sealed class _Answer_79 : Answer<AddToClusterNodeLabelsResponse>
		{
			public _Answer_79(TestRMAdminCLI _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public AddToClusterNodeLabelsResponse Answer(InvocationOnMock invocation)
			{
				this._enclosing.remoteAdminServiceAccessed = true;
				return AddToClusterNodeLabelsResponse.NewInstance();
			}

			private readonly TestRMAdminCLI _enclosing;
		}

		private sealed class _RMAdminCLI_96 : RMAdminCLI
		{
			public _RMAdminCLI_96(TestRMAdminCLI _enclosing, HAServiceTarget haServiceTarget, 
				Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.haServiceTarget = haServiceTarget;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ResourceManagerAdministrationProtocol CreateAdminProtocol
				()
			{
				return this._enclosing.admin;
			}

			protected override HAServiceTarget ResolveTarget(string rmId)
			{
				return haServiceTarget;
			}

			private readonly TestRMAdminCLI _enclosing;

			private readonly HAServiceTarget haServiceTarget;
		}

		private sealed class _RMAdminCLI_113 : RMAdminCLI
		{
			public _RMAdminCLI_113(TestRMAdminCLI _enclosing, HAServiceTarget haServiceTarget
				, Configuration baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.haServiceTarget = haServiceTarget;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override ResourceManagerAdministrationProtocol CreateAdminProtocol
				()
			{
				return this._enclosing.admin;
			}

			protected override HAServiceTarget ResolveTarget(string rmId)
			{
				return haServiceTarget;
			}

			private readonly TestRMAdminCLI _enclosing;

			private readonly HAServiceTarget haServiceTarget;
		}

		private void InitDummyNodeLabelsManager()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NodeLabelsEnabled, true);
			dummyNodeLabelsManager = new DummyCommonNodeLabelsManager();
			dummyNodeLabelsManager.Init(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshQueues()
		{
			string[] args = new string[] { "-refreshQueues" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(admin).RefreshQueues(Matchers.Any<RefreshQueuesRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshUserToGroupsMappings()
		{
			string[] args = new string[] { "-refreshUserToGroupsMappings" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(admin).RefreshUserToGroupsMappings(Matchers.Any<RefreshUserToGroupsMappingsRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshSuperUserGroupsConfiguration()
		{
			string[] args = new string[] { "-refreshSuperUserGroupsConfiguration" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(admin).RefreshSuperUserGroupsConfiguration(Matchers.Any
				<RefreshSuperUserGroupsConfigurationRequest>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshAdminAcls()
		{
			string[] args = new string[] { "-refreshAdminAcls" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(admin).RefreshAdminAcls(Matchers.Any<RefreshAdminAclsRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshServiceAcl()
		{
			string[] args = new string[] { "-refreshServiceAcl" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(admin).RefreshServiceAcls(Matchers.Any<RefreshServiceAclsRequest
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshNodes()
		{
			string[] args = new string[] { "-refreshNodes" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(admin).RefreshNodes(Matchers.Any<RefreshNodesRequest>(
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetGroups()
		{
			Org.Mockito.Mockito.When(admin.GetGroupsForUser(Matchers.Eq("admin"))).ThenReturn
				(new string[] { "group1", "group2" });
			TextWriter origOut = System.Console.Out;
			TextWriter @out = Org.Mockito.Mockito.Mock<TextWriter>();
			Runtime.SetOut(@out);
			try
			{
				string[] args = new string[] { "-getGroups", "admin" };
				NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
				Org.Mockito.Mockito.Verify(admin).GetGroupsForUser(Matchers.Eq("admin"));
				Org.Mockito.Mockito.Verify(@out).WriteLine(Matchers.ArgThat(new _ArgumentMatcher_190
					()));
			}
			finally
			{
				Runtime.SetOut(origOut);
			}
		}

		private sealed class _ArgumentMatcher_190 : ArgumentMatcher<StringBuilder>
		{
			public _ArgumentMatcher_190()
			{
			}

			public override bool Matches(object argument)
			{
				return (string.Empty + argument).Equals("admin : group1 group2");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionToActive()
		{
			string[] args = new string[] { "-transitionToActive", "rm1" };
			// RM HA is disabled.
			// transitionToActive should not be executed
			NUnit.Framework.Assert.AreEqual(-1, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(haadmin, Org.Mockito.Mockito.Never()).TransitionToActive
				(Matchers.Any<HAServiceProtocol.StateChangeRequestInfo>());
			// Now RM HA is enabled.
			// transitionToActive should be executed
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLIWithHAEnabled.Run(args));
			Org.Mockito.Mockito.Verify(haadmin).TransitionToActive(Matchers.Any<HAServiceProtocol.StateChangeRequestInfo
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionToStandby()
		{
			string[] args = new string[] { "-transitionToStandby", "rm1" };
			// RM HA is disabled.
			// transitionToStandby should not be executed
			NUnit.Framework.Assert.AreEqual(-1, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(haadmin, Org.Mockito.Mockito.Never()).TransitionToStandby
				(Matchers.Any<HAServiceProtocol.StateChangeRequestInfo>());
			// Now RM HA is enabled.
			// transitionToActive should be executed
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLIWithHAEnabled.Run(args));
			Org.Mockito.Mockito.Verify(haadmin).TransitionToStandby(Matchers.Any<HAServiceProtocol.StateChangeRequestInfo
				>());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetServiceState()
		{
			string[] args = new string[] { "-getServiceState", "rm1" };
			// RM HA is disabled.
			// getServiceState should not be executed
			NUnit.Framework.Assert.AreEqual(-1, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(haadmin, Org.Mockito.Mockito.Never()).GetServiceStatus
				();
			// Now RM HA is enabled.
			// getServiceState should be executed
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLIWithHAEnabled.Run(args));
			Org.Mockito.Mockito.Verify(haadmin).GetServiceStatus();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckHealth()
		{
			string[] args = new string[] { "-checkHealth", "rm1" };
			// RM HA is disabled.
			// getServiceState should not be executed
			NUnit.Framework.Assert.AreEqual(-1, rmAdminCLI.Run(args));
			Org.Mockito.Mockito.Verify(haadmin, Org.Mockito.Mockito.Never()).MonitorHealth();
			// Now RM HA is enabled.
			// getServiceState should be executed
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLIWithHAEnabled.Run(args));
			Org.Mockito.Mockito.Verify(haadmin).MonitorHealth();
		}

		/// <summary>Test printing of help messages</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestHelp()
		{
			TextWriter oldOutPrintStream = System.Console.Out;
			TextWriter oldErrPrintStream = System.Console.Error;
			ByteArrayOutputStream dataOut = new ByteArrayOutputStream();
			ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
			Runtime.SetOut(new TextWriter(dataOut));
			Runtime.SetErr(new TextWriter(dataErr));
			try
			{
				string[] args = new string[] { "-help" };
				NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
				oldOutPrintStream.WriteLine(dataOut);
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("rmadmin is the command to execute YARN administrative commands."
					));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("yarn rmadmin [-refreshQueues] [-refreshNodes] [-refreshSuper"
					 + "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " + "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup"
					 + " [username]] [[-addToClusterNodeLabels [label1,label2,label3]]" + " [-removeFromClusterNodeLabels [label1,label2,label3]] [-replaceLabelsOnNode "
					 + "[node1[:port]=label1,label2 node2[:port]=label1] [-directlyAccessNodeLabelStore]] "
					 + "[-help [cmd]]"));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-refreshQueues: Reload the queues' acls, states and scheduler "
					 + "specific properties."));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-refreshNodes: Refresh the hosts information at the "
					 + "ResourceManager."));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-refreshUserToGroupsMappings: Refresh user-to-groups mappings"
					));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-refreshSuperUserGroupsConfiguration: Refresh superuser proxy"
					 + " groups mappings"));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-refreshAdminAcls: Refresh acls for administration of "
					 + "ResourceManager"));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-refreshServiceAcl: Reload the service-level authorization"
					 + " policy file"));
				NUnit.Framework.Assert.IsTrue(dataOut.ToString().Contains("-help [cmd]: Displays help for the given command or all "
					 + "commands if none"));
				TestError(new string[] { "-help", "-refreshQueues" }, "Usage: yarn rmadmin [-refreshQueues]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-refreshNodes" }, "Usage: yarn rmadmin [-refreshNodes]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-refreshUserToGroupsMappings" }, "Usage: yarn rmadmin [-refreshUserToGroupsMappings]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-refreshSuperUserGroupsConfiguration" }, "Usage: yarn rmadmin [-refreshSuperUserGroupsConfiguration]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-refreshAdminAcls" }, "Usage: yarn rmadmin [-refreshAdminAcls]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-refreshServiceAcl" }, "Usage: yarn rmadmin [-refreshServiceAcl]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-getGroups" }, "Usage: yarn rmadmin [-getGroups [username]]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-transitionToActive" }, "Usage: yarn rmadmin [-transitionToActive [--forceactive]"
					 + " <serviceId>]", dataErr, 0);
				TestError(new string[] { "-help", "-transitionToStandby" }, "Usage: yarn rmadmin [-transitionToStandby <serviceId>]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-getServiceState" }, "Usage: yarn rmadmin [-getServiceState <serviceId>]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-checkHealth" }, "Usage: yarn rmadmin [-checkHealth <serviceId>]"
					, dataErr, 0);
				TestError(new string[] { "-help", "-failover" }, "Usage: yarn rmadmin " + "[-failover [--forcefence] [--forceactive] "
					 + "<serviceId> <serviceId>]", dataErr, 0);
				TestError(new string[] { "-help", "-badParameter" }, "Usage: yarn rmadmin", dataErr
					, 0);
				TestError(new string[] { "-badParameter" }, "badParameter: Unknown command", dataErr
					, -1);
				// Test -help when RM HA is enabled
				NUnit.Framework.Assert.AreEqual(0, rmAdminCLIWithHAEnabled.Run(args));
				oldOutPrintStream.WriteLine(dataOut);
				string expectedHelpMsg = "yarn rmadmin [-refreshQueues] [-refreshNodes] [-refreshSuper"
					 + "UserGroupsConfiguration] [-refreshUserToGroupsMappings] " + "[-refreshAdminAcls] [-refreshServiceAcl] [-getGroup"
					 + " [username]] [[-addToClusterNodeLabels [label1,label2,label3]]" + " [-removeFromClusterNodeLabels [label1,label2,label3]] [-replaceLabelsOnNode "
					 + "[node1[:port]=label1,label2 node2[:port]=label1] [-directlyAccessNodeLabelStore]] "
					 + "[-transitionToActive [--forceactive] <serviceId>] " + "[-transitionToStandby <serviceId>] [-failover"
					 + " [--forcefence] [--forceactive] <serviceId> <serviceId>] " + "[-getServiceState <serviceId>] [-checkHealth <serviceId>] [-help [cmd]]";
				string actualHelpMsg = dataOut.ToString();
				NUnit.Framework.Assert.IsTrue(string.Format("Help messages: %n " + actualHelpMsg 
					+ " %n doesn't include expected " + "messages: %n" + expectedHelpMsg), actualHelpMsg
					.Contains(expectedHelpMsg));
			}
			finally
			{
				Runtime.SetOut(oldOutPrintStream);
				Runtime.SetErr(oldErrPrintStream);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestException()
		{
			TextWriter oldErrPrintStream = System.Console.Error;
			ByteArrayOutputStream dataErr = new ByteArrayOutputStream();
			Runtime.SetErr(new TextWriter(dataErr));
			try
			{
				Org.Mockito.Mockito.When(admin.RefreshQueues(Matchers.Any<RefreshQueuesRequest>()
					)).ThenThrow(new IOException("test exception"));
				string[] args = new string[] { "-refreshQueues" };
				NUnit.Framework.Assert.AreEqual(-1, rmAdminCLI.Run(args));
				Org.Mockito.Mockito.Verify(admin).RefreshQueues(Matchers.Any<RefreshQueuesRequest
					>());
				NUnit.Framework.Assert.IsTrue(dataErr.ToString().Contains("refreshQueues: test exception"
					));
			}
			finally
			{
				Runtime.SetErr(oldErrPrintStream);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccessLocalNodeLabelManager()
		{
			NUnit.Framework.Assert.IsFalse(dummyNodeLabelsManager.GetServiceState() == Service.STATE
				.Stopped);
			string[] args = new string[] { "-addToClusterNodeLabels", "x,y", "-directlyAccessNodeLabelStore"
				 };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetClusterNodeLabels().ContainsAll
				(ImmutableSet.Of("x", "y")));
			// reset localNodeLabelsManager
			dummyNodeLabelsManager.RemoveFromClusterNodeLabels(ImmutableSet.Of("x", "y"));
			// change the sequence of "-directlyAccessNodeLabelStore" and labels,
			// should not matter
			args = new string[] { "-addToClusterNodeLabels", "-directlyAccessNodeLabelStore", 
				"x,y" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetClusterNodeLabels().ContainsAll
				(ImmutableSet.Of("x", "y")));
			// local node labels manager will be close after running
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetServiceState() == Service.STATE
				.Stopped);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAccessRemoteNodeLabelManager()
		{
			string[] args = new string[] { "-addToClusterNodeLabels", "x,y" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			// localNodeLabelsManager shouldn't accessed
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetClusterNodeLabels().IsEmpty
				());
			// remote node labels manager accessed
			NUnit.Framework.Assert.IsTrue(remoteAdminServiceAccessed);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddToClusterNodeLabels()
		{
			// successfully add labels
			string[] args = new string[] { "-addToClusterNodeLabels", "x", "-directlyAccessNodeLabelStore"
				 };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetClusterNodeLabels().ContainsAll
				(ImmutableSet.Of("x")));
			// no labels, should fail
			args = new string[] { "-addToClusterNodeLabels" };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail
			args = new string[] { "-addToClusterNodeLabels", "-directlyAccessNodeLabelStore" };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail at client validation
			args = new string[] { "-addToClusterNodeLabels", " " };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail at client validation
			args = new string[] { "-addToClusterNodeLabels", " , " };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// successfully add labels
			args = new string[] { "-addToClusterNodeLabels", ",x,,", "-directlyAccessNodeLabelStore"
				 };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetClusterNodeLabels().ContainsAll
				(ImmutableSet.Of("x")));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemoveFromClusterNodeLabels()
		{
			// Successfully remove labels
			dummyNodeLabelsManager.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			string[] args = new string[] { "-removeFromClusterNodeLabels", "x,,y", "-directlyAccessNodeLabelStore"
				 };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetClusterNodeLabels().IsEmpty
				());
			// no labels, should fail
			args = new string[] { "-removeFromClusterNodeLabels" };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail
			args = new string[] { "-removeFromClusterNodeLabels", "-directlyAccessNodeLabelStore"
				 };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail at client validation
			args = new string[] { "-removeFromClusterNodeLabels", " " };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail at client validation
			args = new string[] { "-removeFromClusterNodeLabels", ", " };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceLabelsOnNode()
		{
			// Successfully replace labels
			dummyNodeLabelsManager.AddToCluserNodeLabels(ImmutableSet.Of("x", "y", "Y"));
			string[] args = new string[] { "-replaceLabelsOnNode", "node1:8000,x node2:8000=y node3,x node4=Y"
				, "-directlyAccessNodeLabelStore" };
			NUnit.Framework.Assert.AreEqual(0, rmAdminCLI.Run(args));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetNodeLabels().Contains(NodeId
				.NewInstance("node1", 8000)));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetNodeLabels().Contains(NodeId
				.NewInstance("node2", 8000)));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetNodeLabels().Contains(NodeId
				.NewInstance("node3", 0)));
			NUnit.Framework.Assert.IsTrue(dummyNodeLabelsManager.GetNodeLabels().Contains(NodeId
				.NewInstance("node4", 0)));
			// no labels, should fail
			args = new string[] { "-replaceLabelsOnNode" };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail
			args = new string[] { "-replaceLabelsOnNode", "-directlyAccessNodeLabelStore" };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			// no labels, should fail
			args = new string[] { "-replaceLabelsOnNode", " " };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
			args = new string[] { "-replaceLabelsOnNode", ", " };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplaceMultipleLabelsOnSingleNode()
		{
			// Successfully replace labels
			dummyNodeLabelsManager.AddToCluserNodeLabels(ImmutableSet.Of("x", "y"));
			string[] args = new string[] { "-replaceLabelsOnNode", "node1,x,y", "-directlyAccessNodeLabelStore"
				 };
			NUnit.Framework.Assert.IsTrue(0 != rmAdminCLI.Run(args));
		}

		/// <exception cref="System.Exception"/>
		private void TestError(string[] args, string template, ByteArrayOutputStream data
			, int resultCode)
		{
			int actualResultCode = rmAdminCLI.Run(args);
			NUnit.Framework.Assert.AreEqual("Expected result code: " + resultCode + ", actual result code is: "
				 + actualResultCode, resultCode, actualResultCode);
			NUnit.Framework.Assert.IsTrue(string.Format("Expected error message: %n" + template
				 + " is not included in messages: %n" + data.ToString()), data.ToString().Contains
				(template));
			data.Reset();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMHAErrorUsage()
		{
			ByteArrayOutputStream errOutBytes = new ByteArrayOutputStream();
			rmAdminCLIWithHAEnabled.SetErrOut(new TextWriter(errOutBytes));
			try
			{
				string[] args = new string[] { "-failover" };
				NUnit.Framework.Assert.AreEqual(-1, rmAdminCLIWithHAEnabled.Run(args));
				string errOut = new string(errOutBytes.ToByteArray(), Charsets.Utf8);
				errOutBytes.Reset();
				NUnit.Framework.Assert.IsTrue(errOut.Contains("Usage: rmadmin"));
			}
			finally
			{
				rmAdminCLIWithHAEnabled.SetErrOut(System.Console.Error);
			}
		}
	}
}
