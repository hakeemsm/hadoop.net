using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server
{
	public class TestRMNMSecretKeys
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestNMUpdation()
		{
			YarnConfiguration conf = new YarnConfiguration();
			// validating RM NM keys for Unsecured environment
			ValidateRMNMKeyExchange(conf);
			// validating RM NM keys for secured environment
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			ValidateRMNMKeyExchange(conf);
		}

		/// <exception cref="System.Exception"/>
		private void ValidateRMNMKeyExchange(YarnConfiguration conf)
		{
			// Default rolling and activation intervals are large enough, no need to
			// intervene
			DrainDispatcher dispatcher = new DrainDispatcher();
			ResourceManager rm = new _ResourceManager_56(dispatcher);
			// Do nothing.
			// Don't need it, skip.
			rm.Init(conf);
			rm.Start();
			// Testing ContainerToken and NMToken
			string containerToken = "Container Token : ";
			string nmToken = "NM Token : ";
			MockNM nm = new MockNM("host:1234", 3072, rm.GetResourceTrackerService());
			RegisterNodeManagerResponse registrationResponse = nm.RegisterNode();
			MasterKey containerTokenMasterKey = registrationResponse.GetContainerTokenMasterKey
				();
			NUnit.Framework.Assert.IsNotNull(containerToken + "Registration should cause a key-update!"
				, containerTokenMasterKey);
			MasterKey nmTokenMasterKey = registrationResponse.GetNMTokenMasterKey();
			NUnit.Framework.Assert.IsNotNull(nmToken + "Registration should cause a key-update!"
				, nmTokenMasterKey);
			dispatcher.Await();
			NodeHeartbeatResponse response = nm.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsNull(containerToken + "First heartbeat after registration shouldn't get any key updates!"
				, response.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.IsNull(nmToken + "First heartbeat after registration shouldn't get any key updates!"
				, response.GetNMTokenMasterKey());
			dispatcher.Await();
			response = nm.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsNull(containerToken + "Even second heartbeat after registration shouldn't get any key updates!"
				, response.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.IsNull(nmToken + "Even second heartbeat after registration shouldn't get any key updates!"
				, response.GetContainerTokenMasterKey());
			dispatcher.Await();
			// Let's force a roll-over
			rm.GetRMContext().GetContainerTokenSecretManager().RollMasterKey();
			rm.GetRMContext().GetNMTokenSecretManager().RollMasterKey();
			// Heartbeats after roll-over and before activation should be fine.
			response = nm.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsNotNull(containerToken + "Heartbeats after roll-over and before activation should not err out."
				, response.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.IsNotNull(nmToken + "Heartbeats after roll-over and before activation should not err out."
				, response.GetNMTokenMasterKey());
			NUnit.Framework.Assert.AreEqual(containerToken + "Roll-over should have incremented the key-id only by one!"
				, containerTokenMasterKey.GetKeyId() + 1, response.GetContainerTokenMasterKey().
				GetKeyId());
			NUnit.Framework.Assert.AreEqual(nmToken + "Roll-over should have incremented the key-id only by one!"
				, nmTokenMasterKey.GetKeyId() + 1, response.GetNMTokenMasterKey().GetKeyId());
			dispatcher.Await();
			response = nm.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsNull(containerToken + "Second heartbeat after roll-over shouldn't get any key updates!"
				, response.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.IsNull(nmToken + "Second heartbeat after roll-over shouldn't get any key updates!"
				, response.GetNMTokenMasterKey());
			dispatcher.Await();
			// Let's force activation
			rm.GetRMContext().GetContainerTokenSecretManager().ActivateNextMasterKey();
			rm.GetRMContext().GetNMTokenSecretManager().ActivateNextMasterKey();
			response = nm.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsNull(containerToken + "Activation shouldn't cause any key updates!"
				, response.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.IsNull(nmToken + "Activation shouldn't cause any key updates!"
				, response.GetNMTokenMasterKey());
			dispatcher.Await();
			response = nm.NodeHeartbeat(true);
			NUnit.Framework.Assert.IsNull(containerToken + "Even second heartbeat after activation shouldn't get any key updates!"
				, response.GetContainerTokenMasterKey());
			NUnit.Framework.Assert.IsNull(nmToken + "Even second heartbeat after activation shouldn't get any key updates!"
				, response.GetNMTokenMasterKey());
			dispatcher.Await();
			rm.Stop();
		}

		private sealed class _ResourceManager_56 : ResourceManager
		{
			public _ResourceManager_56(DrainDispatcher dispatcher)
			{
				this.dispatcher = dispatcher;
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void DoSecureLogin()
			{
			}

			protected override Dispatcher CreateDispatcher()
			{
				return dispatcher;
			}

			protected override void StartWepApp()
			{
			}

			private readonly DrainDispatcher dispatcher;
		}
	}
}
