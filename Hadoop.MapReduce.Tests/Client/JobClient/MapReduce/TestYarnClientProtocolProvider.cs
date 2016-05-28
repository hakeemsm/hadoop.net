using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestYarnClientProtocolProvider : TestCase
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterWithYarnClientProtocolProvider()
		{
			Configuration conf = new Configuration(false);
			Cluster cluster = null;
			try
			{
				cluster = new Cluster(conf);
			}
			catch (Exception e)
			{
				throw new Exception("Failed to initialize a local runner w/o a cluster framework key"
					, e);
			}
			try
			{
				NUnit.Framework.Assert.IsTrue("client is not a LocalJobRunner", cluster.GetClient
					() is LocalJobRunner);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Close();
				}
			}
			try
			{
				conf = new Configuration();
				conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
				cluster = new Cluster(conf);
				ClientProtocol client = cluster.GetClient();
				NUnit.Framework.Assert.IsTrue("client is a YARNRunner", client is YARNRunner);
			}
			catch (IOException)
			{
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterGetDelegationToken()
		{
			Configuration conf = new Configuration(false);
			Cluster cluster = null;
			try
			{
				conf = new Configuration();
				conf.Set(MRConfig.FrameworkName, MRConfig.YarnFrameworkName);
				cluster = new Cluster(conf);
				YARNRunner yrunner = (YARNRunner)cluster.GetClient();
				GetDelegationTokenResponse getDTResponse = recordFactory.NewRecordInstance<GetDelegationTokenResponse
					>();
				Token rmDTToken = recordFactory.NewRecordInstance<Token>();
				rmDTToken.SetIdentifier(ByteBuffer.Wrap(new byte[2]));
				rmDTToken.SetKind("Testclusterkind");
				rmDTToken.SetPassword(ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("testcluster"
					)));
				rmDTToken.SetService("0.0.0.0:8032");
				getDTResponse.SetRMDelegationToken(rmDTToken);
				ApplicationClientProtocol cRMProtocol = Org.Mockito.Mockito.Mock<ApplicationClientProtocol
					>();
				Org.Mockito.Mockito.When(cRMProtocol.GetDelegationToken(Matchers.Any<GetDelegationTokenRequest
					>())).ThenReturn(getDTResponse);
				ResourceMgrDelegate rmgrDelegate = new _ResourceMgrDelegate_112(cRMProtocol, new 
					YarnConfiguration(conf));
				yrunner.SetResourceMgrDelegate(rmgrDelegate);
				Org.Apache.Hadoop.Security.Token.Token t = cluster.GetDelegationToken(new Text(" "
					));
				NUnit.Framework.Assert.IsTrue("Token kind is instead " + t.GetKind().ToString(), 
					"Testclusterkind".Equals(t.GetKind().ToString()));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Close();
				}
			}
		}

		private sealed class _ResourceMgrDelegate_112 : ResourceMgrDelegate
		{
			public _ResourceMgrDelegate_112(ApplicationClientProtocol cRMProtocol, YarnConfiguration
				 baseArg1)
				: base(baseArg1)
			{
				this.cRMProtocol = cRMProtocol;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				NUnit.Framework.Assert.IsTrue(this.client is YarnClientImpl);
				((YarnClientImpl)this.client).SetRMClient(cRMProtocol);
			}

			private readonly ApplicationClientProtocol cRMProtocol;
		}
	}
}
