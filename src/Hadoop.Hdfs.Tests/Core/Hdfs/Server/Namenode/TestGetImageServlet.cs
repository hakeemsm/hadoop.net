using Javax.Servlet;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestGetImageServlet
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestIsValidRequestor()
		{
			Configuration conf = new HdfsConfiguration();
			KerberosName.SetRules("RULE:[1:$1]\nRULE:[2:$1]");
			// Set up generic HA configs.
			conf.Set(DFSConfigKeys.DfsNameservices, "ns1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, "ns1"), "nn1,nn2"
				);
			// Set up NN1 HA configs.
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn1"
				), "host1:1234");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey, "ns1"
				, "nn1"), "hdfs/_HOST@TEST-REALM.COM");
			// Set up NN2 HA configs.
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, "ns1", "nn2"
				), "host2:1234");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeKerberosPrincipalKey, "ns1"
				, "nn2"), "hdfs/_HOST@TEST-REALM.COM");
			// Initialize this conf object as though we're running on NN1.
			NameNode.InitializeGenericKeys(conf, "ns1", "nn1");
			AccessControlList acls = Org.Mockito.Mockito.Mock<AccessControlList>();
			Org.Mockito.Mockito.When(acls.IsUserAllowed(Org.Mockito.Mockito.Any<UserGroupInformation
				>())).ThenReturn(false);
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			Org.Mockito.Mockito.When(context.GetAttribute(HttpServer2.AdminsAcl)).ThenReturn(
				acls);
			// Make sure that NN2 is considered a valid fsimage/edits requestor.
			NUnit.Framework.Assert.IsTrue(ImageServlet.IsValidRequestor(context, "hdfs/host2@TEST-REALM.COM"
				, conf));
			// Mark atm as an admin.
			Org.Mockito.Mockito.When(acls.IsUserAllowed(Org.Mockito.Mockito.ArgThat(new _ArgumentMatcher_76
				()))).ThenReturn(true);
			// Make sure that NN2 is still considered a valid requestor.
			NUnit.Framework.Assert.IsTrue(ImageServlet.IsValidRequestor(context, "hdfs/host2@TEST-REALM.COM"
				, conf));
			// Make sure an admin is considered a valid requestor.
			NUnit.Framework.Assert.IsTrue(ImageServlet.IsValidRequestor(context, "atm@TEST-REALM.COM"
				, conf));
			// Make sure other users are *not* considered valid requestors.
			NUnit.Framework.Assert.IsFalse(ImageServlet.IsValidRequestor(context, "todd@TEST-REALM.COM"
				, conf));
		}

		private sealed class _ArgumentMatcher_76 : ArgumentMatcher<UserGroupInformation>
		{
			public _ArgumentMatcher_76()
			{
			}

			public override bool Matches(object argument)
			{
				return ((UserGroupInformation)argument).GetShortUserName().Equals("atm");
			}
		}
	}
}
