using System;
using System.Collections.Generic;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Api.Client.Config;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.Servlet;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp
{
	public class TestTimelineWebServices : JerseyTestBase
	{
		private static TimelineStore store;

		private static TimelineACLsManager timelineACLsManager;

		private static AdminACLsManager adminACLsManager;

		private long beforeTime;

		private sealed class _ServletModule_85 : ServletModule
		{
			public _ServletModule_85(TestTimelineWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this.Bind<YarnJacksonJaxbJsonProvider>();
				this.Bind<TimelineWebServices>();
				this.Bind<GenericExceptionHandler>();
				try
				{
					Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices.store = this
						._enclosing.MockTimelineStore();
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail();
				}
				Configuration conf = new YarnConfiguration();
				conf.SetBoolean(YarnConfiguration.YarnAclEnable, false);
				Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices.timelineACLsManager
					 = new TimelineACLsManager(conf);
				Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices.timelineACLsManager
					.SetTimelineStore(Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices
					.store);
				conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
				conf.Set(YarnConfiguration.YarnAdminAcl, "admin");
				Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices.adminACLsManager
					 = new AdminACLsManager(conf);
				TimelineDataManager timelineDataManager = new TimelineDataManager(Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices
					.store, Org.Apache.Hadoop.Yarn.Server.Timeline.Webapp.TestTimelineWebServices.timelineACLsManager
					);
				timelineDataManager.Init(conf);
				timelineDataManager.Start();
				this.Bind<TimelineDataManager>().ToInstance(timelineDataManager);
				this.Serve("/*").With(typeof(GuiceContainer));
				TimelineAuthenticationFilter taFilter = new TimelineAuthenticationFilter();
				FilterConfig filterConfig = Org.Mockito.Mockito.Mock<FilterConfig>();
				Org.Mockito.Mockito.When(filterConfig.GetInitParameter(AuthenticationFilter.ConfigPrefix
					)).ThenReturn(null);
				Org.Mockito.Mockito.When(filterConfig.GetInitParameter(AuthenticationFilter.AuthType
					)).ThenReturn("simple");
				Org.Mockito.Mockito.When(filterConfig.GetInitParameter(PseudoAuthenticationHandler
					.AnonymousAllowed)).ThenReturn("true");
				ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
				Org.Mockito.Mockito.When(filterConfig.GetServletContext()).ThenReturn(context);
				Enumeration<object> names = Org.Mockito.Mockito.Mock<IEnumeration>();
				Org.Mockito.Mockito.When(names.MoveNext()).ThenReturn(true, true, true, false);
				Org.Mockito.Mockito.When(names.Current).ThenReturn(AuthenticationFilter.AuthType, 
					PseudoAuthenticationHandler.AnonymousAllowed, DelegationTokenAuthenticationHandler
					.TokenKind);
				Org.Mockito.Mockito.When(filterConfig.GetInitParameterNames()).ThenReturn(names);
				Org.Mockito.Mockito.When(filterConfig.GetInitParameter(DelegationTokenAuthenticationHandler
					.TokenKind)).ThenReturn(TimelineDelegationTokenIdentifier.KindName.ToString());
				try
				{
					taFilter.Init(filterConfig);
				}
				catch (ServletException e)
				{
					NUnit.Framework.Assert.Fail("Unable to initialize TimelineAuthenticationFilter: "
						 + e.Message);
				}
				taFilter = Org.Mockito.Mockito.Spy(taFilter);
				try
				{
					Org.Mockito.Mockito.DoNothing().When(taFilter).Init(Matchers.Any<FilterConfig>());
				}
				catch (ServletException e)
				{
					NUnit.Framework.Assert.Fail("Unable to initialize TimelineAuthenticationFilter: "
						 + e.Message);
				}
				this.Filter("/*").Through(taFilter);
			}

			private readonly TestTimelineWebServices _enclosing;
		}

		private Injector injector;

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestTimelineWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestTimelineWebServices _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private TimelineStore MockTimelineStore()
		{
			beforeTime = Runtime.CurrentTimeMillis() - 1;
			TestMemoryTimelineStore store = new TestMemoryTimelineStore();
			store.Setup();
			return store.GetTimelineStore();
		}

		public TestTimelineWebServices()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.applicationhistoryservice.webapp"
				).ContextListenerClass(typeof(TestTimelineWebServices.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").ClientConfig
				(new DefaultClientConfig(typeof(YarnJacksonJaxbJsonProvider))).Build())
		{
			injector = Guice.CreateInjector(new _ServletModule_85(this));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAbout()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineWebServices.AboutInfo about = response.GetEntity<TimelineWebServices.AboutInfo
				>();
			NUnit.Framework.Assert.IsNotNull(about);
			NUnit.Framework.Assert.AreEqual("Timeline API", about.GetAbout());
		}

		private static void VerifyEntities(TimelineEntities entities)
		{
			NUnit.Framework.Assert.IsNotNull(entities);
			NUnit.Framework.Assert.AreEqual(3, entities.GetEntities().Count);
			TimelineEntity entity1 = entities.GetEntities()[0];
			NUnit.Framework.Assert.IsNotNull(entity1);
			NUnit.Framework.Assert.AreEqual("id_1", entity1.GetEntityId());
			NUnit.Framework.Assert.AreEqual("type_1", entity1.GetEntityType());
			NUnit.Framework.Assert.AreEqual(123l, entity1.GetStartTime());
			NUnit.Framework.Assert.AreEqual(2, entity1.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(4, entity1.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(4, entity1.GetOtherInfo().Count);
			TimelineEntity entity2 = entities.GetEntities()[1];
			NUnit.Framework.Assert.IsNotNull(entity2);
			NUnit.Framework.Assert.AreEqual("id_2", entity2.GetEntityId());
			NUnit.Framework.Assert.AreEqual("type_1", entity2.GetEntityType());
			NUnit.Framework.Assert.AreEqual(123l, entity2.GetStartTime());
			NUnit.Framework.Assert.AreEqual(2, entity2.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(4, entity2.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(4, entity2.GetOtherInfo().Count);
			TimelineEntity entity3 = entities.GetEntities()[2];
			NUnit.Framework.Assert.IsNotNull(entity2);
			NUnit.Framework.Assert.AreEqual("id_6", entity3.GetEntityId());
			NUnit.Framework.Assert.AreEqual("type_1", entity3.GetEntityType());
			NUnit.Framework.Assert.AreEqual(61l, entity3.GetStartTime());
			NUnit.Framework.Assert.AreEqual(0, entity3.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(4, entity3.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(4, entity3.GetOtherInfo().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntities()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			VerifyEntities(response.GetEntity<TimelineEntities>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFromId()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("fromId", "id_2").Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(2, response.GetEntity<TimelineEntities>().GetEntities
				().Count);
			response = r.Path("ws").Path("v1").Path("timeline").Path("type_1").QueryParam("fromId"
				, "id_1").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(3, response.GetEntity<TimelineEntities>().GetEntities
				().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFromTs()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("fromTs", System.Convert.ToString(beforeTime)).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(0, response.GetEntity<TimelineEntities>().GetEntities
				().Count);
			response = r.Path("ws").Path("v1").Path("timeline").Path("type_1").QueryParam("fromTs"
				, System.Convert.ToString(Runtime.CurrentTimeMillis())).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(3, response.GetEntity<TimelineEntities>().GetEntities
				().Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestPrimaryFilterString()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("primaryFilter", "user:username").Accept(MediaType.ApplicationJson).
				Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			VerifyEntities(response.GetEntity<TimelineEntities>());
		}

		[NUnit.Framework.Test]
		public virtual void TestPrimaryFilterInteger()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("primaryFilter", "appname:" + Sharpen.Extensions.ToString(int.MaxValue
				)).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			VerifyEntities(response.GetEntity<TimelineEntities>());
		}

		[NUnit.Framework.Test]
		public virtual void TestPrimaryFilterLong()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("primaryFilter", "long:" + System.Convert.ToString((long)int.MaxValue
				 + 1l)).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			VerifyEntities(response.GetEntity<TimelineEntities>());
		}

		[NUnit.Framework.Test]
		public virtual void TestPrimaryFilterNumericString()
		{
			// without quotes, 123abc is interpreted as the number 123,
			// which finds no entities
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("primaryFilter", "other:123abc").Accept(MediaType.ApplicationJson).Get
				<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(0, response.GetEntity<TimelineEntities>().GetEntities
				().Count);
		}

		[NUnit.Framework.Test]
		public virtual void TestPrimaryFilterNumericStringWithQuotes()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("primaryFilter", "other:\"123abc\"").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			VerifyEntities(response.GetEntity<TimelineEntities>());
		}

		[NUnit.Framework.Test]
		public virtual void TestSecondaryFilters()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.QueryParam("secondaryFilter", "user:username,appname:" + Sharpen.Extensions.ToString
				(int.MaxValue)).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			VerifyEntities(response.GetEntity<TimelineEntities>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntity()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.Path("id_1").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineEntity entity = response.GetEntity<TimelineEntity>();
			NUnit.Framework.Assert.IsNotNull(entity);
			NUnit.Framework.Assert.AreEqual("id_1", entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual("type_1", entity.GetEntityType());
			NUnit.Framework.Assert.AreEqual(123l, entity.GetStartTime());
			NUnit.Framework.Assert.AreEqual(2, entity.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(4, entity.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(4, entity.GetOtherInfo().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntityFields1()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.Path("id_1").QueryParam("fields", "events,otherinfo").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineEntity entity = response.GetEntity<TimelineEntity>();
			NUnit.Framework.Assert.IsNotNull(entity);
			NUnit.Framework.Assert.AreEqual("id_1", entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual("type_1", entity.GetEntityType());
			NUnit.Framework.Assert.AreEqual(123l, entity.GetStartTime());
			NUnit.Framework.Assert.AreEqual(2, entity.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(0, entity.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(4, entity.GetOtherInfo().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntityFields2()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.Path("id_1").QueryParam("fields", "lasteventonly," + "primaryfilters,relatedentities"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineEntity entity = response.GetEntity<TimelineEntity>();
			NUnit.Framework.Assert.IsNotNull(entity);
			NUnit.Framework.Assert.AreEqual("id_1", entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual("type_1", entity.GetEntityType());
			NUnit.Framework.Assert.AreEqual(123l, entity.GetStartTime());
			NUnit.Framework.Assert.AreEqual(1, entity.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(4, entity.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(0, entity.GetOtherInfo().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEvents()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("type_1")
				.Path("events").QueryParam("entityId", "id_1").Accept(MediaType.ApplicationJson)
				.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineEvents events = response.GetEntity<TimelineEvents>();
			NUnit.Framework.Assert.IsNotNull(events);
			NUnit.Framework.Assert.AreEqual(1, events.GetAllEvents().Count);
			TimelineEvents.EventsOfOneEntity partEvents = events.GetAllEvents()[0];
			NUnit.Framework.Assert.AreEqual(2, partEvents.GetEvents().Count);
			TimelineEvent event1 = partEvents.GetEvents()[0];
			NUnit.Framework.Assert.AreEqual(456l, event1.GetTimestamp());
			NUnit.Framework.Assert.AreEqual("end_event", event1.GetEventType());
			NUnit.Framework.Assert.AreEqual(1, event1.GetEventInfo().Count);
			TimelineEvent event2 = partEvents.GetEvents()[1];
			NUnit.Framework.Assert.AreEqual(123l, event2.GetTimestamp());
			NUnit.Framework.Assert.AreEqual("start_event", event2.GetEventType());
			NUnit.Framework.Assert.AreEqual(0, event2.GetEventInfo().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntitiesWithPrimaryFilter()
		{
			TimelineEntities entities = new TimelineEntities();
			TimelineEntity entity = new TimelineEntity();
			IDictionary<string, ICollection<object>> filters = new Dictionary<string, ICollection
				<object>>();
			filters[TimelineStore.SystemFilter.EntityOwner.ToString()] = new HashSet<object>(
				);
			entity.SetPrimaryFilters(filters);
			entity.SetEntityId("test id 6");
			entity.SetEntityType("test type 6");
			entity.SetStartTime(Runtime.CurrentTimeMillis());
			entities.AddEntity(entity);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name"
				, "tester").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Post
				<ClientResponse>(entities);
			TimelinePutResponse putResposne = response.GetEntity<TimelinePutResponse>();
			NUnit.Framework.Assert.AreEqual(0, putResposne.GetErrors().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntities()
		{
			TimelineEntities entities = new TimelineEntities();
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityId("test id 1");
			entity.SetEntityType("test type 1");
			entity.SetStartTime(Runtime.CurrentTimeMillis());
			entity.SetDomainId("domain_id_1");
			entities.AddEntity(entity);
			WebResource r = Resource();
			// No owner, will be rejected
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Accept(MediaType
				.ApplicationJson).Type(MediaType.ApplicationJson).Post<ClientResponse>(entities);
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
				());
			response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name", "tester"
				).Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Post<ClientResponse
				>(entities);
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelinePutResponse putResposne = response.GetEntity<TimelinePutResponse>();
			NUnit.Framework.Assert.IsNotNull(putResposne);
			NUnit.Framework.Assert.AreEqual(0, putResposne.GetErrors().Count);
			// verify the entity exists in the store
			response = r.Path("ws").Path("v1").Path("timeline").Path("test type 1").Path("test id 1"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			entity = response.GetEntity<TimelineEntity>();
			NUnit.Framework.Assert.IsNotNull(entity);
			NUnit.Framework.Assert.AreEqual("test id 1", entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual("test type 1", entity.GetEntityType());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntitiesWithYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				TimelineEntities entities = new TimelineEntities();
				TimelineEntity entity = new TimelineEntity();
				entity.SetEntityId("test id 2");
				entity.SetEntityType("test type 2");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_1");
				entities.AddEntity(entity);
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name"
					, "writer_user_1").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson
					).Post<ClientResponse>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelinePutResponse putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.IsNotNull(putResponse);
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				// override/append timeline data in the same entity with different user
				response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name", "writer_user_3"
					).Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Post<ClientResponse
					>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.IsNotNull(putResponse);
				NUnit.Framework.Assert.AreEqual(1, putResponse.GetErrors().Count);
				NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.AccessDenied
					, putResponse.GetErrors()[0].GetErrorCode());
				// Cross domain relationship will be rejected
				entities = new TimelineEntities();
				entity = new TimelineEntity();
				entity.SetEntityId("test id 3");
				entity.SetEntityType("test type 2");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_2");
				entity.SetRelatedEntities(Sharpen.Collections.SingletonMap("test type 2", Sharpen.Collections
					.Singleton("test id 2")));
				entities.AddEntity(entity);
				r = Resource();
				response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name", "writer_user_3"
					).Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Post<ClientResponse
					>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.IsNotNull(putResponse);
				NUnit.Framework.Assert.AreEqual(1, putResponse.GetErrors().Count);
				NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.ForbiddenRelation
					, putResponse.GetErrors()[0].GetErrorCode());
				// Make sure the entity has been added anyway even though the
				// relationship is been excluded
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 2").Path("test id 3"
					).QueryParam("user.name", "reader_user_3").Accept(MediaType.ApplicationJson).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				entity = response.GetEntity<TimelineEntity>();
				NUnit.Framework.Assert.IsNotNull(entity);
				NUnit.Framework.Assert.AreEqual("test id 3", entity.GetEntityId());
				NUnit.Framework.Assert.AreEqual("test type 2", entity.GetEntityType());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPostEntitiesToDefaultDomain()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				TimelineEntities entities = new TimelineEntities();
				TimelineEntity entity = new TimelineEntity();
				entity.SetEntityId("test id 7");
				entity.SetEntityType("test type 7");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entities.AddEntity(entity);
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name"
					, "anybody_1").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson)
					.Post<ClientResponse>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelinePutResponse putResposne = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.IsNotNull(putResposne);
				NUnit.Framework.Assert.AreEqual(0, putResposne.GetErrors().Count);
				// verify the entity exists in the store
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 7").Path("test id 7"
					).QueryParam("user.name", "any_body_2").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				entity = response.GetEntity<TimelineEntity>();
				NUnit.Framework.Assert.IsNotNull(entity);
				NUnit.Framework.Assert.AreEqual("test id 7", entity.GetEntityId());
				NUnit.Framework.Assert.AreEqual("test type 7", entity.GetEntityType());
				NUnit.Framework.Assert.AreEqual(TimelineDataManager.DefaultDomainId, entity.GetDomainId
					());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntityWithYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				TimelineEntities entities = new TimelineEntities();
				TimelineEntity entity = new TimelineEntity();
				entity.SetEntityId("test id 3");
				entity.SetEntityType("test type 3");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_1");
				entities.AddEntity(entity);
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name"
					, "writer_user_1").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson
					).Post<ClientResponse>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelinePutResponse putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				// verify the system data will not be exposed
				// 1. No field specification
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 3").Path("test id 3"
					).QueryParam("user.name", "reader_user_1").Accept(MediaType.ApplicationJson).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				entity = response.GetEntity<TimelineEntity>();
				NUnit.Framework.Assert.IsNull(entity.GetPrimaryFilters()[TimelineStore.SystemFilter
					.EntityOwner.ToString()]);
				// 2. other field
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 3").Path("test id 3"
					).QueryParam("fields", "relatedentities").QueryParam("user.name", "reader_user_1"
					).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				entity = response.GetEntity<TimelineEntity>();
				NUnit.Framework.Assert.IsNull(entity.GetPrimaryFilters()[TimelineStore.SystemFilter
					.EntityOwner.ToString()]);
				// 3. primaryfilters field
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 3").Path("test id 3"
					).QueryParam("fields", "primaryfilters").QueryParam("user.name", "reader_user_1"
					).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				entity = response.GetEntity<TimelineEntity>();
				NUnit.Framework.Assert.IsNull(entity.GetPrimaryFilters()[TimelineStore.SystemFilter
					.EntityOwner.ToString()]);
				// get entity with other user
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 3").Path("test id 3"
					).QueryParam("user.name", "reader_user_2").Accept(MediaType.ApplicationJson).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGetEntitiesWithYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				// Put entity [4, 4] in domain 1
				TimelineEntities entities = new TimelineEntities();
				TimelineEntity entity = new TimelineEntity();
				entity.SetEntityId("test id 4");
				entity.SetEntityType("test type 4");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_1");
				entities.AddEntity(entity);
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name"
					, "writer_user_1").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson
					).Post<ClientResponse>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelinePutResponse putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				// Put entity [4, 5] in domain 2
				entities = new TimelineEntities();
				entity = new TimelineEntity();
				entity.SetEntityId("test id 5");
				entity.SetEntityType("test type 4");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_2");
				entities.AddEntity(entity);
				r = Resource();
				response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name", "writer_user_3"
					).Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Post<ClientResponse
					>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				// Query entities of type 4
				response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name", "reader_user_1"
					).Path("test type 4").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				entities = response.GetEntity<TimelineEntities>();
				// Reader 1 should just have the access to entity [4, 4]
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				NUnit.Framework.Assert.AreEqual("test type 4", entities.GetEntities()[0].GetEntityType
					());
				NUnit.Framework.Assert.AreEqual("test id 4", entities.GetEntities()[0].GetEntityId
					());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestGetEventsWithYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				// Put entity [5, 5] in domain 1
				TimelineEntities entities = new TimelineEntities();
				TimelineEntity entity = new TimelineEntity();
				entity.SetEntityId("test id 5");
				entity.SetEntityType("test type 5");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_1");
				TimelineEvent @event = new TimelineEvent();
				@event.SetEventType("event type 1");
				@event.SetTimestamp(Runtime.CurrentTimeMillis());
				entity.AddEvent(@event);
				entities.AddEntity(entity);
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name"
					, "writer_user_1").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson
					).Post<ClientResponse>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelinePutResponse putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				// Put entity [5, 6] in domain 2
				entities = new TimelineEntities();
				entity = new TimelineEntity();
				entity.SetEntityId("test id 6");
				entity.SetEntityType("test type 5");
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				entity.SetDomainId("domain_id_2");
				@event = new TimelineEvent();
				@event.SetEventType("event type 2");
				@event.SetTimestamp(Runtime.CurrentTimeMillis());
				entity.AddEvent(@event);
				entities.AddEntity(entity);
				r = Resource();
				response = r.Path("ws").Path("v1").Path("timeline").QueryParam("user.name", "writer_user_3"
					).Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Post<ClientResponse
					>(entities);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				putResponse = response.GetEntity<TimelinePutResponse>();
				NUnit.Framework.Assert.AreEqual(0, putResponse.GetErrors().Count);
				// Query events belonging to the entities of type 4
				response = r.Path("ws").Path("v1").Path("timeline").Path("test type 5").Path("events"
					).QueryParam("user.name", "reader_user_1").QueryParam("entityId", "test id 5,test id 6"
					).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelineEvents events = response.GetEntity<TimelineEvents>();
				// Reader 1 should just have the access to the events of entity [5, 5]
				NUnit.Framework.Assert.AreEqual(1, events.GetAllEvents().Count);
				NUnit.Framework.Assert.AreEqual("test id 5", events.GetAllEvents()[0].GetEntityId
					());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDomain()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("domain")
				.Path("domain_id_1").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineDomain domain = response.GetEntity<TimelineDomain>();
			VerifyDomain(domain, "domain_id_1");
		}

		[NUnit.Framework.Test]
		public virtual void TestGetDomainYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("domain")
					.Path("domain_id_1").QueryParam("user.name", "owner_1").Accept(MediaType.ApplicationJson
					).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelineDomain domain = response.GetEntity<TimelineDomain>();
				VerifyDomain(domain, "domain_id_1");
				response = r.Path("ws").Path("v1").Path("timeline").Path("domain").Path("domain_id_1"
					).QueryParam("user.name", "tester").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDomains()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("domain")
				.QueryParam("owner", "owner_1").Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			TimelineDomains domains = response.GetEntity<TimelineDomains>();
			NUnit.Framework.Assert.AreEqual(2, domains.GetDomains().Count);
			for (int i = 0; i < domains.GetDomains().Count; ++i)
			{
				VerifyDomain(domains.GetDomains()[i], i == 0 ? "domain_id_4" : "domain_id_1");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetDomainsYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("domain")
					.QueryParam("user.name", "owner_1").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				TimelineDomains domains = response.GetEntity<TimelineDomains>();
				NUnit.Framework.Assert.AreEqual(2, domains.GetDomains().Count);
				for (int i = 0; i < domains.GetDomains().Count; ++i)
				{
					VerifyDomain(domains.GetDomains()[i], i == 0 ? "domain_id_4" : "domain_id_1");
				}
				response = r.Path("ws").Path("v1").Path("timeline").Path("domain").QueryParam("owner"
					, "owner_1").QueryParam("user.name", "tester").Accept(MediaType.ApplicationJson)
					.Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				domains = response.GetEntity<TimelineDomains>();
				NUnit.Framework.Assert.AreEqual(0, domains.GetDomains().Count);
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutDomain()
		{
			TimelineDomain domain = new TimelineDomain();
			domain.SetId("test_domain_id");
			WebResource r = Resource();
			// No owner, will be rejected
			ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("domain")
				.Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Put<ClientResponse
				>(domain);
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
				());
			response = r.Path("ws").Path("v1").Path("timeline").Path("domain").QueryParam("user.name"
				, "tester").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Put
				<ClientResponse>(domain);
			NUnit.Framework.Assert.AreEqual(Response.Status.Ok.GetStatusCode(), response.GetStatus
				());
			// Verify the domain exists
			response = r.Path("ws").Path("v1").Path("timeline").Path("domain").Path("test_domain_id"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			domain = response.GetEntity<TimelineDomain>();
			NUnit.Framework.Assert.IsNotNull(domain);
			NUnit.Framework.Assert.AreEqual("test_domain_id", domain.GetId());
			NUnit.Framework.Assert.AreEqual("tester", domain.GetOwner());
			NUnit.Framework.Assert.AreEqual(null, domain.GetDescription());
			// Update the domain
			domain.SetDescription("test_description");
			response = r.Path("ws").Path("v1").Path("timeline").Path("domain").QueryParam("user.name"
				, "tester").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Put
				<ClientResponse>(domain);
			NUnit.Framework.Assert.AreEqual(Response.Status.Ok.GetStatusCode(), response.GetStatus
				());
			// Verify the domain is updated
			response = r.Path("ws").Path("v1").Path("timeline").Path("domain").Path("test_domain_id"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			domain = response.GetEntity<TimelineDomain>();
			NUnit.Framework.Assert.IsNotNull(domain);
			NUnit.Framework.Assert.AreEqual("test_domain_id", domain.GetId());
			NUnit.Framework.Assert.AreEqual("test_description", domain.GetDescription());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPutDomainYarnACLsEnabled()
		{
			AdminACLsManager oldAdminACLsManager = timelineACLsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				TimelineDomain domain = new TimelineDomain();
				domain.SetId("test_domain_id_acl");
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("timeline").Path("domain")
					.QueryParam("user.name", "tester").Accept(MediaType.ApplicationJson).Type(MediaType
					.ApplicationJson).Put<ClientResponse>(domain);
				NUnit.Framework.Assert.AreEqual(Response.Status.Ok.GetStatusCode(), response.GetStatus
					());
				// Update the domain by another user
				response = r.Path("ws").Path("v1").Path("timeline").Path("domain").QueryParam("user.name"
					, "other").Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson).Put
					<ClientResponse>(domain);
				NUnit.Framework.Assert.AreEqual(Response.Status.Forbidden.GetStatusCode(), response
					.GetStatus());
			}
			finally
			{
				timelineACLsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		private static void VerifyDomain(TimelineDomain domain, string domainId)
		{
			NUnit.Framework.Assert.IsNotNull(domain);
			NUnit.Framework.Assert.AreEqual(domainId, domain.GetId());
			// The specific values have been verified in TestMemoryTimelineStore
			NUnit.Framework.Assert.IsNotNull(domain.GetDescription());
			NUnit.Framework.Assert.IsNotNull(domain.GetOwner());
			NUnit.Framework.Assert.IsNotNull(domain.GetReaders());
			NUnit.Framework.Assert.IsNotNull(domain.GetWriters());
			NUnit.Framework.Assert.IsNotNull(domain.GetCreatedTime());
			NUnit.Framework.Assert.IsNotNull(domain.GetModifiedTime());
		}
	}
}
