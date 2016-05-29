using System;
using Com.Sun.Jersey.Api.Client;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestTimelineClient
	{
		private TimelineClientImpl client;

		[SetUp]
		public virtual void Setup()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			client = CreateTimelineClient(conf);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (client != null)
			{
				client.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntities()
		{
			MockEntityClientResponse(client, ClientResponse.Status.Ok, false, false);
			try
			{
				TimelinePutResponse response = client.PutEntities(GenerateEntity());
				NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expected");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntitiesWithError()
		{
			MockEntityClientResponse(client, ClientResponse.Status.Ok, true, false);
			try
			{
				TimelinePutResponse response = client.PutEntities(GenerateEntity());
				NUnit.Framework.Assert.AreEqual(1, response.GetErrors().Count);
				NUnit.Framework.Assert.AreEqual("test entity id", response.GetErrors()[0].GetEntityId
					());
				NUnit.Framework.Assert.AreEqual("test entity type", response.GetErrors()[0].GetEntityType
					());
				NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.IoException, 
					response.GetErrors()[0].GetErrorCode());
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expected");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntitiesNoResponse()
		{
			MockEntityClientResponse(client, ClientResponse.Status.InternalServerError, false
				, false);
			try
			{
				client.PutEntities(GenerateEntity());
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Failed to get the response from the timeline server."
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntitiesConnectionRefused()
		{
			MockEntityClientResponse(client, null, false, true);
			try
			{
				client.PutEntities(GenerateEntity());
				NUnit.Framework.Assert.Fail("RuntimeException is expected");
			}
			catch (RuntimeException re)
			{
				NUnit.Framework.Assert.IsTrue(re is ClientHandlerException);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutDomain()
		{
			MockDomainClientResponse(client, ClientResponse.Status.Ok, false);
			try
			{
				client.PutDomain(GenerateDomain());
			}
			catch (YarnException)
			{
				NUnit.Framework.Assert.Fail("Exception is not expected");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutDomainNoResponse()
		{
			MockDomainClientResponse(client, ClientResponse.Status.Forbidden, false);
			try
			{
				client.PutDomain(GenerateDomain());
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Failed to get the response from the timeline server."
					));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutDomainConnectionRefused()
		{
			MockDomainClientResponse(client, null, true);
			try
			{
				client.PutDomain(GenerateDomain());
				NUnit.Framework.Assert.Fail("RuntimeException is expected");
			}
			catch (RuntimeException re)
			{
				NUnit.Framework.Assert.IsTrue(re is ClientHandlerException);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckRetryCount()
		{
			try
			{
				YarnConfiguration conf = new YarnConfiguration();
				conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
				conf.SetInt(YarnConfiguration.TimelineServiceClientMaxRetries, -2);
				CreateTimelineClient(conf);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceClientMaxRetries
					));
			}
			try
			{
				YarnConfiguration conf = new YarnConfiguration();
				conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
				conf.SetLong(YarnConfiguration.TimelineServiceClientRetryIntervalMs, 0);
				CreateTimelineClient(conf);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceClientRetryIntervalMs
					));
			}
			int newMaxRetries = 5;
			long newIntervalMs = 500;
			YarnConfiguration conf_1 = new YarnConfiguration();
			conf_1.SetInt(YarnConfiguration.TimelineServiceClientMaxRetries, newMaxRetries);
			conf_1.SetLong(YarnConfiguration.TimelineServiceClientRetryIntervalMs, newIntervalMs
				);
			conf_1.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			TimelineClientImpl client = CreateTimelineClient(conf_1);
			try
			{
				// This call should fail because there is no timeline server
				client.PutEntities(GenerateEntity());
				NUnit.Framework.Assert.Fail("Exception expected! " + "Timeline server should be off to run this test. "
					);
			}
			catch (RuntimeException ce)
			{
				NUnit.Framework.Assert.IsTrue("Handler exception for reason other than retry: " +
					 ce.Message, ce.Message.Contains("Connection retries limit exceeded"));
				// we would expect this exception here, check if the client has retried
				NUnit.Framework.Assert.IsTrue("Retry filter didn't perform any retries! ", client
					.connectionRetry.GetRetired());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDelegationTokenOperationsRetry()
		{
			int newMaxRetries = 5;
			long newIntervalMs = 500;
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.TimelineServiceClientMaxRetries, newMaxRetries);
			conf.SetLong(YarnConfiguration.TimelineServiceClientRetryIntervalMs, newIntervalMs
				);
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			// use kerberos to bypass the issue in HADOOP-11215
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			TimelineClientImpl client = CreateTimelineClient(conf);
			TestTimelineClient.TestTimlineDelegationTokenSecretManager dtManager = new TestTimelineClient.TestTimlineDelegationTokenSecretManager
				();
			try
			{
				dtManager.StartThreads();
				Sharpen.Thread.Sleep(3000);
				try
				{
					// try getting a delegation token
					client.GetDelegationToken(UserGroupInformation.GetCurrentUser().GetShortUserName(
						));
					AssertFail();
				}
				catch (RuntimeException ce)
				{
					AssertException(client, ce);
				}
				try
				{
					// try renew a delegation token
					TimelineDelegationTokenIdentifier timelineDT = new TimelineDelegationTokenIdentifier
						(new Text("tester"), new Text("tester"), new Text("tester"));
					client.RenewDelegationToken(new Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
						>(timelineDT.GetBytes(), dtManager.CreatePassword(timelineDT), timelineDT.GetKind
						(), new Text("0.0.0.0:8188")));
					AssertFail();
				}
				catch (RuntimeException ce)
				{
					AssertException(client, ce);
				}
				try
				{
					// try cancel a delegation token
					TimelineDelegationTokenIdentifier timelineDT = new TimelineDelegationTokenIdentifier
						(new Text("tester"), new Text("tester"), new Text("tester"));
					client.CancelDelegationToken(new Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
						>(timelineDT.GetBytes(), dtManager.CreatePassword(timelineDT), timelineDT.GetKind
						(), new Text("0.0.0.0:8188")));
					AssertFail();
				}
				catch (RuntimeException ce)
				{
					AssertException(client, ce);
				}
			}
			finally
			{
				client.Stop();
				dtManager.StopThreads();
			}
		}

		private static void AssertFail()
		{
			NUnit.Framework.Assert.Fail("Exception expected! " + "Timeline server should be off to run this test."
				);
		}

		private void AssertException(TimelineClientImpl client, RuntimeException ce)
		{
			NUnit.Framework.Assert.IsTrue("Handler exception for reason other than retry: " +
				 ce.ToString(), ce.Message.Contains("Connection retries limit exceeded"));
			// we would expect this exception here, check if the client has retried
			NUnit.Framework.Assert.IsTrue("Retry filter didn't perform any retries! ", client
				.connectionRetry.GetRetired());
		}

		private static ClientResponse MockEntityClientResponse(TimelineClientImpl client, 
			ClientResponse.Status status, bool hasError, bool hasRuntimeError)
		{
			ClientResponse response = Org.Mockito.Mockito.Mock<ClientResponse>();
			if (hasRuntimeError)
			{
				Org.Mockito.Mockito.DoThrow(new ClientHandlerException(new ConnectException())).When
					(client).DoPostingObject(Matchers.Any<TimelineEntities>(), Matchers.Any<string>(
					));
				return response;
			}
			Org.Mockito.Mockito.DoReturn(response).When(client).DoPostingObject(Matchers.Any<
				TimelineEntities>(), Matchers.Any<string>());
			Org.Mockito.Mockito.When(response.GetClientResponseStatus()).ThenReturn(status);
			TimelinePutResponse.TimelinePutError error = new TimelinePutResponse.TimelinePutError
				();
			error.SetEntityId("test entity id");
			error.SetEntityType("test entity type");
			error.SetErrorCode(TimelinePutResponse.TimelinePutError.IoException);
			TimelinePutResponse putResponse = new TimelinePutResponse();
			if (hasError)
			{
				putResponse.AddError(error);
			}
			Org.Mockito.Mockito.When(response.GetEntity<TimelinePutResponse>()).ThenReturn(putResponse
				);
			return response;
		}

		private static ClientResponse MockDomainClientResponse(TimelineClientImpl client, 
			ClientResponse.Status status, bool hasRuntimeError)
		{
			ClientResponse response = Org.Mockito.Mockito.Mock<ClientResponse>();
			if (hasRuntimeError)
			{
				Org.Mockito.Mockito.DoThrow(new ClientHandlerException(new ConnectException())).When
					(client).DoPostingObject(Matchers.Any<TimelineDomain>(), Matchers.Any<string>());
				return response;
			}
			Org.Mockito.Mockito.DoReturn(response).When(client).DoPostingObject(Matchers.Any<
				TimelineDomain>(), Matchers.Any<string>());
			Org.Mockito.Mockito.When(response.GetClientResponseStatus()).ThenReturn(status);
			return response;
		}

		private static TimelineEntity GenerateEntity()
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityId("entity id");
			entity.SetEntityType("entity type");
			entity.SetStartTime(Runtime.CurrentTimeMillis());
			for (int i = 0; i < 2; ++i)
			{
				TimelineEvent @event = new TimelineEvent();
				@event.SetTimestamp(Runtime.CurrentTimeMillis());
				@event.SetEventType("test event type " + i);
				@event.AddEventInfo("key1", "val1");
				@event.AddEventInfo("key2", "val2");
				entity.AddEvent(@event);
			}
			entity.AddRelatedEntity("test ref type 1", "test ref id 1");
			entity.AddRelatedEntity("test ref type 2", "test ref id 2");
			entity.AddPrimaryFilter("pkey1", "pval1");
			entity.AddPrimaryFilter("pkey2", "pval2");
			entity.AddOtherInfo("okey1", "oval1");
			entity.AddOtherInfo("okey2", "oval2");
			entity.SetDomainId("domain id 1");
			return entity;
		}

		public static TimelineDomain GenerateDomain()
		{
			TimelineDomain domain = new TimelineDomain();
			domain.SetId("namesapce id");
			domain.SetDescription("domain description");
			domain.SetOwner("domain owner");
			domain.SetReaders("domain_reader");
			domain.SetWriters("domain_writer");
			domain.SetCreatedTime(0L);
			domain.SetModifiedTime(1L);
			return domain;
		}

		private static TimelineClientImpl CreateTimelineClient(YarnConfiguration conf)
		{
			TimelineClientImpl client = Org.Mockito.Mockito.Spy((TimelineClientImpl)TimelineClient
				.CreateTimelineClient());
			client.Init(conf);
			client.Start();
			return client;
		}

		private class TestTimlineDelegationTokenSecretManager : AbstractDelegationTokenSecretManager
			<TimelineDelegationTokenIdentifier>
		{
			public TestTimlineDelegationTokenSecretManager()
				: base(100000, 100000, 100000, 100000)
			{
			}

			public override TimelineDelegationTokenIdentifier CreateIdentifier()
			{
				return new TimelineDelegationTokenIdentifier();
			}

			protected override byte[] CreatePassword(TimelineDelegationTokenIdentifier identifier
				)
			{
				lock (this)
				{
					return base.CreatePassword(identifier);
				}
			}
		}
	}
}
