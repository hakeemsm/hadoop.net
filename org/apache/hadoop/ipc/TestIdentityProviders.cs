using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class TestIdentityProviders
	{
		public class FakeSchedulable : org.apache.hadoop.ipc.Schedulable
		{
			public FakeSchedulable(TestIdentityProviders _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public virtual org.apache.hadoop.security.UserGroupInformation getUserGroupInformation
				()
			{
				try
				{
					return org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
				}
				catch (System.IO.IOException)
				{
					return null;
				}
			}

			private readonly TestIdentityProviders _enclosing;
		}

		[NUnit.Framework.Test]
		public virtual void testPluggableIdentityProvider()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CALLQUEUE_IDENTITY_PROVIDER_KEY
				, "org.apache.hadoop.ipc.UserIdentityProvider");
			System.Collections.Generic.IList<org.apache.hadoop.ipc.IdentityProvider> providers
				 = conf.getInstances<org.apache.hadoop.ipc.IdentityProvider>(org.apache.hadoop.fs.CommonConfigurationKeys
				.IPC_CALLQUEUE_IDENTITY_PROVIDER_KEY);
			NUnit.Framework.Assert.IsTrue(providers.Count == 1);
			org.apache.hadoop.ipc.IdentityProvider ip = providers[0];
			NUnit.Framework.Assert.IsNotNull(ip);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForObject(ip), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.UserIdentityProvider)));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testUserIdentityProvider()
		{
			org.apache.hadoop.ipc.UserIdentityProvider uip = new org.apache.hadoop.ipc.UserIdentityProvider
				();
			string identity = uip.makeIdentity(new org.apache.hadoop.ipc.TestIdentityProviders.FakeSchedulable
				(this));
			// Get our username
			org.apache.hadoop.security.UserGroupInformation ugi = org.apache.hadoop.security.UserGroupInformation
				.getCurrentUser();
			string username = ugi.getUserName();
			NUnit.Framework.Assert.AreEqual(username, identity);
		}
	}
}
