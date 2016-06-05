using System;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Api.Client.Config;
using Com.Sun.Jersey.Api.Client.Filter;
using Com.Sun.Jersey.Client.Urlconnection;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Cli;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jackson.Map;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TimelineClientImpl : TimelineClient
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.TimelineClientImpl
			));

		private const string ResourceUriStr = "/ws/v1/timeline/";

		private static readonly Joiner Joiner = Joiner.On(string.Empty);

		public const int DefaultSocketTimeout = 1 * 60 * 1000;

		private static Options opts;

		private const string EntityDataType = "entity";

		private const string DomainDataType = "domain";

		static TimelineClientImpl()
		{
			// 1 minute
			opts = new Options();
			opts.AddOption("put", true, "Put the timeline entities/domain in a JSON file");
			opts.GetOption("put").SetArgName("Path to the JSON file");
			opts.AddOption(EntityDataType, false, "Specify the JSON file contains the entities"
				);
			opts.AddOption(DomainDataType, false, "Specify the JSON file contains the domain"
				);
			opts.AddOption("help", false, "Print usage");
		}

		private Com.Sun.Jersey.Api.Client.Client client;

		private ConnectionConfigurator connConfigurator;

		private DelegationTokenAuthenticator authenticator;

		private DelegationTokenAuthenticatedURL.Token token;

		private URI resURI;

		private UserGroupInformation authUgi;

		private string doAsUser;

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal TimelineClientImpl.TimelineClientConnectionRetry connectionRetry;

		private abstract class TimelineClientRetryOp
		{
			// Abstract class for an operation that should be retried by timeline client
			// The operation that should be retried
			/// <exception cref="System.IO.IOException"/>
			public abstract object Run();

			// The method to indicate if we should retry given the incoming exception
			public abstract bool ShouldRetryOn(Exception e);
		}

		internal class TimelineClientConnectionRetry
		{
			[InterfaceAudience.Private]
			[VisibleForTesting]
			public int maxRetries;

			[InterfaceAudience.Private]
			[VisibleForTesting]
			public long retryInterval;

			private bool retried = false;

			// Class to handle retry
			// Outside this class, only visible to tests
			// maxRetries < 0 means keep trying
			// Indicates if retries happened last time. Only tests should read it.
			// In unit tests, retryOn() calls should _not_ be concurrent.
			[InterfaceAudience.Private]
			[VisibleForTesting]
			internal virtual bool GetRetired()
			{
				return retried;
			}

			public TimelineClientConnectionRetry(Configuration conf)
			{
				// Constructor with default retry settings
				Preconditions.CheckArgument(conf.GetInt(YarnConfiguration.TimelineServiceClientMaxRetries
					, YarnConfiguration.DefaultTimelineServiceClientMaxRetries) >= -1, "%s property value should be greater than or equal to -1"
					, YarnConfiguration.TimelineServiceClientMaxRetries);
				Preconditions.CheckArgument(conf.GetLong(YarnConfiguration.TimelineServiceClientRetryIntervalMs
					, YarnConfiguration.DefaultTimelineServiceClientRetryIntervalMs) > 0, "%s property value should be greater than zero"
					, YarnConfiguration.TimelineServiceClientRetryIntervalMs);
				maxRetries = conf.GetInt(YarnConfiguration.TimelineServiceClientMaxRetries, YarnConfiguration
					.DefaultTimelineServiceClientMaxRetries);
				retryInterval = conf.GetLong(YarnConfiguration.TimelineServiceClientRetryIntervalMs
					, YarnConfiguration.DefaultTimelineServiceClientRetryIntervalMs);
			}

			/// <exception cref="Sharpen.RuntimeException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual object RetryOn(TimelineClientImpl.TimelineClientRetryOp op)
			{
				int leftRetries = maxRetries;
				retried = false;
				// keep trying
				while (true)
				{
					try
					{
						// try perform the op, if fail, keep retrying
						return op.Run();
					}
					catch (Exception e)
					{
						// break if there's no retries left
						if (leftRetries == 0)
						{
							break;
						}
						if (op.ShouldRetryOn(e))
						{
							LogException(e, leftRetries);
						}
						else
						{
							throw;
						}
					}
					if (leftRetries > 0)
					{
						leftRetries--;
					}
					retried = true;
					try
					{
						// sleep for the given time interval
						Sharpen.Thread.Sleep(retryInterval);
					}
					catch (Exception)
					{
						Log.Warn("Client retry sleep interrupted! ");
					}
				}
				throw new RuntimeException("Failed to connect to timeline server. " + "Connection retries limit exceeded. "
					 + "The posted timeline event may be missing");
			}

			private void LogException(Exception e, int leftRetries)
			{
				if (leftRetries > 0)
				{
					Log.Info("Exception caught by TimelineClientConnectionRetry," + " will try " + leftRetries
						 + " more time(s).\nMessage: " + e.Message);
				}
				else
				{
					// note that maxRetries may be -1 at the very beginning
					Log.Info("ConnectionException caught by TimelineClientConnectionRetry," + " will keep retrying.\nMessage: "
						 + e.Message);
				}
			}
		}

		private class TimelineJerseyRetryFilter : ClientFilter
		{
			/// <exception cref="Com.Sun.Jersey.Api.Client.ClientHandlerException"/>
			public override ClientResponse Handle(ClientRequest cr)
			{
				// Set up the retry operation
				TimelineClientImpl.TimelineClientRetryOp jerseyRetryOp = new _TimelineClientRetryOp_230
					(this, cr);
				// Try pass the request, if fail, keep retrying
				// Only retry on connection exceptions
				try
				{
					return (ClientResponse)this._enclosing.connectionRetry.RetryOn(jerseyRetryOp);
				}
				catch (IOException e)
				{
					throw new ClientHandlerException("Jersey retry failed!\nMessage: " + e.Message);
				}
			}

			private sealed class _TimelineClientRetryOp_230 : TimelineClientImpl.TimelineClientRetryOp
			{
				public _TimelineClientRetryOp_230(TimelineJerseyRetryFilter _enclosing, ClientRequest
					 cr)
				{
					this._enclosing = _enclosing;
					this.cr = cr;
				}

				public override object Run()
				{
					return this._enclosing.GetNext().Handle(cr);
				}

				public override bool ShouldRetryOn(Exception e)
				{
					return (e is ClientHandlerException) && (e.InnerException is ConnectException);
				}

				private readonly TimelineJerseyRetryFilter _enclosing;

				private readonly ClientRequest cr;
			}

			internal TimelineJerseyRetryFilter(TimelineClientImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TimelineClientImpl _enclosing;
		}

		public TimelineClientImpl()
			: base(typeof(TimelineClientImpl).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			UserGroupInformation realUgi = ugi.GetRealUser();
			if (realUgi != null)
			{
				authUgi = realUgi;
				doAsUser = ugi.GetShortUserName();
			}
			else
			{
				authUgi = ugi;
				doAsUser = null;
			}
			ClientConfig cc = new DefaultClientConfig();
			cc.GetClasses().AddItem(typeof(YarnJacksonJaxbJsonProvider));
			connConfigurator = NewConnConfigurator(conf);
			if (UserGroupInformation.IsSecurityEnabled())
			{
				authenticator = new KerberosDelegationTokenAuthenticator();
			}
			else
			{
				authenticator = new PseudoDelegationTokenAuthenticator();
			}
			authenticator.SetConnectionConfigurator(connConfigurator);
			token = new DelegationTokenAuthenticatedURL.Token();
			connectionRetry = new TimelineClientImpl.TimelineClientConnectionRetry(conf);
			client = new Com.Sun.Jersey.Api.Client.Client(new URLConnectionClientHandler(new 
				TimelineClientImpl.TimelineURLConnectionFactory(this)), cc);
			TimelineClientImpl.TimelineJerseyRetryFilter retryFilter = new TimelineClientImpl.TimelineJerseyRetryFilter
				(this);
			client.AddFilter(retryFilter);
			if (YarnConfiguration.UseHttps(conf))
			{
				resURI = URI.Create(Joiner.Join("https://", conf.Get(YarnConfiguration.TimelineServiceWebappHttpsAddress
					, YarnConfiguration.DefaultTimelineServiceWebappHttpsAddress), ResourceUriStr));
			}
			else
			{
				resURI = URI.Create(Joiner.Join("http://", conf.Get(YarnConfiguration.TimelineServiceWebappAddress
					, YarnConfiguration.DefaultTimelineServiceWebappAddress), ResourceUriStr));
			}
			Log.Info("Timeline service address: " + resURI);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override TimelinePutResponse PutEntities(params TimelineEntity[] entities)
		{
			TimelineEntities entitiesContainer = new TimelineEntities();
			entitiesContainer.AddEntities(Arrays.AsList(entities));
			ClientResponse resp = DoPosting(entitiesContainer, null);
			return resp.GetEntity<TimelinePutResponse>();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void PutDomain(TimelineDomain domain)
		{
			DoPosting(domain, "domain");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private ClientResponse DoPosting(object obj, string path)
		{
			ClientResponse resp;
			try
			{
				resp = authUgi.DoAs(new _PrivilegedExceptionAction_320(this, obj, path));
			}
			catch (UndeclaredThrowableException e)
			{
				throw new IOException(e.InnerException);
			}
			catch (Exception ie)
			{
				throw new IOException(ie);
			}
			if (resp == null || resp.GetClientResponseStatus() != ClientResponse.Status.Ok)
			{
				string msg = "Failed to get the response from the timeline server.";
				Log.Error(msg);
				if (Log.IsDebugEnabled() && resp != null)
				{
					string output = resp.GetEntity<string>();
					Log.Debug("HTTP error code: " + resp.GetStatus() + " Server response : \n" + output
						);
				}
				throw new YarnException(msg);
			}
			return resp;
		}

		private sealed class _PrivilegedExceptionAction_320 : PrivilegedExceptionAction<ClientResponse
			>
		{
			public _PrivilegedExceptionAction_320(TimelineClientImpl _enclosing, object obj, 
				string path)
			{
				this._enclosing = _enclosing;
				this.obj = obj;
				this.path = path;
			}

			/// <exception cref="System.Exception"/>
			public ClientResponse Run()
			{
				return this._enclosing.DoPostingObject(obj, path);
			}

			private readonly TimelineClientImpl _enclosing;

			private readonly object obj;

			private readonly string path;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
			> GetDelegationToken(string renewer)
		{
			PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
				>> getDTAction = new _PrivilegedExceptionAction_351(this, renewer);
			return (Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier>
				)OperateDelegationToken(getDTAction);
		}

		private sealed class _PrivilegedExceptionAction_351 : PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token
			<TimelineDelegationTokenIdentifier>>
		{
			public _PrivilegedExceptionAction_351(TimelineClientImpl _enclosing, string renewer
				)
			{
				this._enclosing = _enclosing;
				this.renewer = renewer;
			}

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier> 
				Run()
			{
				DelegationTokenAuthenticatedURL authUrl = new DelegationTokenAuthenticatedURL(this
					._enclosing.authenticator, this._enclosing.connConfigurator);
				return (Org.Apache.Hadoop.Security.Token.Token)authUrl.GetDelegationToken(this._enclosing
					.resURI.ToURL(), this._enclosing.token, renewer, this._enclosing.doAsUser);
			}

			private readonly TimelineClientImpl _enclosing;

			private readonly string renewer;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			TimelineDelegationTokenIdentifier> timelineDT)
		{
			bool isTokenServiceAddrEmpty = timelineDT.GetService().ToString().IsEmpty();
			string scheme = isTokenServiceAddrEmpty ? null : (YarnConfiguration.UseHttps(this
				.GetConfig()) ? "https" : "http");
			IPEndPoint address = isTokenServiceAddrEmpty ? null : SecurityUtil.GetTokenServiceAddr
				(timelineDT);
			PrivilegedExceptionAction<long> renewDTAction = new _PrivilegedExceptionAction_378
				(this, timelineDT, isTokenServiceAddrEmpty, scheme, address);
			// If the timeline DT to renew is different than cached, replace it.
			// Token to set every time for retry, because when exception happens,
			// DelegationTokenAuthenticatedURL will reset it to null;
			// If the token service address is not available, fall back to use
			// the configured service address.
			return (long)OperateDelegationToken(renewDTAction);
		}

		private sealed class _PrivilegedExceptionAction_378 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_378(TimelineClientImpl _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<TimelineDelegationTokenIdentifier> timelineDT, bool isTokenServiceAddrEmpty, string
				 scheme, IPEndPoint address)
			{
				this._enclosing = _enclosing;
				this.timelineDT = timelineDT;
				this.isTokenServiceAddrEmpty = isTokenServiceAddrEmpty;
				this.scheme = scheme;
				this.address = address;
			}

			/// <exception cref="System.Exception"/>
			public long Run()
			{
				if (!timelineDT.Equals(this._enclosing.token.GetDelegationToken()))
				{
					this._enclosing.token.SetDelegationToken((Org.Apache.Hadoop.Security.Token.Token)
						timelineDT);
				}
				DelegationTokenAuthenticatedURL authUrl = new DelegationTokenAuthenticatedURL(this
					._enclosing.authenticator, this._enclosing.connConfigurator);
				URI serviceURI = isTokenServiceAddrEmpty ? this._enclosing.resURI : new URI(scheme
					, null, address.GetHostName(), address.Port, TimelineClientImpl.ResourceUriStr, 
					null, null);
				return authUrl.RenewDelegationToken(serviceURI.ToURL(), this._enclosing.token, this
					._enclosing.doAsUser);
			}

			private readonly TimelineClientImpl _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
				> timelineDT;

			private readonly bool isTokenServiceAddrEmpty;

			private readonly string scheme;

			private readonly IPEndPoint address;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<TimelineDelegationTokenIdentifier> timelineDT)
		{
			bool isTokenServiceAddrEmpty = timelineDT.GetService().ToString().IsEmpty();
			string scheme = isTokenServiceAddrEmpty ? null : (YarnConfiguration.UseHttps(this
				.GetConfig()) ? "https" : "http");
			IPEndPoint address = isTokenServiceAddrEmpty ? null : SecurityUtil.GetTokenServiceAddr
				(timelineDT);
			PrivilegedExceptionAction<Void> cancelDTAction = new _PrivilegedExceptionAction_415
				(this, timelineDT, isTokenServiceAddrEmpty, scheme, address);
			// If the timeline DT to cancel is different than cached, replace it.
			// Token to set every time for retry, because when exception happens,
			// DelegationTokenAuthenticatedURL will reset it to null;
			// If the token service address is not available, fall back to use
			// the configured service address.
			OperateDelegationToken(cancelDTAction);
		}

		private sealed class _PrivilegedExceptionAction_415 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_415(TimelineClientImpl _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<TimelineDelegationTokenIdentifier> timelineDT, bool isTokenServiceAddrEmpty, string
				 scheme, IPEndPoint address)
			{
				this._enclosing = _enclosing;
				this.timelineDT = timelineDT;
				this.isTokenServiceAddrEmpty = isTokenServiceAddrEmpty;
				this.scheme = scheme;
				this.address = address;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				if (!timelineDT.Equals(this._enclosing.token.GetDelegationToken()))
				{
					this._enclosing.token.SetDelegationToken((Org.Apache.Hadoop.Security.Token.Token)
						timelineDT);
				}
				DelegationTokenAuthenticatedURL authUrl = new DelegationTokenAuthenticatedURL(this
					._enclosing.authenticator, this._enclosing.connConfigurator);
				URI serviceURI = isTokenServiceAddrEmpty ? this._enclosing.resURI : new URI(scheme
					, null, address.GetHostName(), address.Port, TimelineClientImpl.ResourceUriStr, 
					null, null);
				authUrl.CancelDelegationToken(serviceURI.ToURL(), this._enclosing.token, this._enclosing
					.doAsUser);
				return null;
			}

			private readonly TimelineClientImpl _enclosing;

			private readonly Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
				> timelineDT;

			private readonly bool isTokenServiceAddrEmpty;

			private readonly string scheme;

			private readonly IPEndPoint address;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private object OperateDelegationToken<_T0>(PrivilegedExceptionAction<_T0> action)
		{
			// Set up the retry operation
			TimelineClientImpl.TimelineClientRetryOp tokenRetryOp = new _TimelineClientRetryOp_444
				(this, action);
			// Try pass the request, if fail, keep retrying
			// Only retry on connection exceptions
			return connectionRetry.RetryOn(tokenRetryOp);
		}

		private sealed class _TimelineClientRetryOp_444 : TimelineClientImpl.TimelineClientRetryOp
		{
			public _TimelineClientRetryOp_444(TimelineClientImpl _enclosing, PrivilegedExceptionAction
				<object> action)
			{
				this._enclosing = _enclosing;
				this.action = action;
			}

			/// <exception cref="System.IO.IOException"/>
			public override object Run()
			{
				this._enclosing.authUgi.CheckTGTAndReloginFromKeytab();
				try
				{
					return this._enclosing.authUgi.DoAs(action);
				}
				catch (UndeclaredThrowableException e)
				{
					throw new IOException(e.InnerException);
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}

			public override bool ShouldRetryOn(Exception e)
			{
				return (e is ConnectException);
			}

			private readonly TimelineClientImpl _enclosing;

			private readonly PrivilegedExceptionAction<object> action;
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public virtual ClientResponse DoPostingObject(object @object, string path)
		{
			WebResource webResource = client.Resource(resURI);
			if (path == null)
			{
				return webResource.Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson
					).Post<ClientResponse>(@object);
			}
			else
			{
				if (path.Equals("domain"))
				{
					return webResource.Path(path).Accept(MediaType.ApplicationJson).Type(MediaType.ApplicationJson
						).Put<ClientResponse>(@object);
				}
				else
				{
					throw new YarnRuntimeException("Unknown resource type");
				}
			}
		}

		private class TimelineURLConnectionFactory : HttpURLConnectionFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual HttpURLConnection GetHttpURLConnection(Uri url)
			{
				this._enclosing.authUgi.CheckTGTAndReloginFromKeytab();
				try
				{
					return new DelegationTokenAuthenticatedURL(this._enclosing.authenticator, this._enclosing
						.connConfigurator).OpenConnection(url, this._enclosing.token, this._enclosing.doAsUser
						);
				}
				catch (UndeclaredThrowableException e)
				{
					throw new IOException(e.InnerException);
				}
				catch (AuthenticationException ae)
				{
					throw new IOException(ae);
				}
			}

			internal TimelineURLConnectionFactory(TimelineClientImpl _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TimelineClientImpl _enclosing;
		}

		private static ConnectionConfigurator NewConnConfigurator(Configuration conf)
		{
			try
			{
				return NewSslConnConfigurator(DefaultSocketTimeout, conf);
			}
			catch (Exception e)
			{
				Log.Debug("Cannot load customized ssl related configuration. " + "Fallback to system-generic settings."
					, e);
				return DefaultTimeoutConnConfigurator;
			}
		}

		private sealed class _ConnectionConfigurator_516 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_516()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				TimelineClientImpl.SetTimeouts(conn, TimelineClientImpl.DefaultSocketTimeout);
				return conn;
			}
		}

		private static readonly ConnectionConfigurator DefaultTimeoutConnConfigurator = new 
			_ConnectionConfigurator_516();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		private static ConnectionConfigurator NewSslConnConfigurator(int timeout, Configuration
			 conf)
		{
			SSLFactory factory;
			SSLSocketFactory sf;
			HostnameVerifier hv;
			factory = new SSLFactory(SSLFactory.Mode.Client, conf);
			factory.Init();
			sf = factory.CreateSSLSocketFactory();
			hv = factory.GetHostnameVerifier();
			return new _ConnectionConfigurator_536(sf, hv, timeout);
		}

		private sealed class _ConnectionConfigurator_536 : ConnectionConfigurator
		{
			public _ConnectionConfigurator_536(SSLSocketFactory sf, HostnameVerifier hv, int 
				timeout)
			{
				this.sf = sf;
				this.hv = hv;
				this.timeout = timeout;
			}

			/// <exception cref="System.IO.IOException"/>
			public HttpURLConnection Configure(HttpURLConnection conn)
			{
				if (conn is HttpsURLConnection)
				{
					HttpsURLConnection c = (HttpsURLConnection)conn;
					c.SetSSLSocketFactory(sf);
					c.SetHostnameVerifier(hv);
				}
				TimelineClientImpl.SetTimeouts(conn, timeout);
				return conn;
			}

			private readonly SSLSocketFactory sf;

			private readonly HostnameVerifier hv;

			private readonly int timeout;
		}

		private static void SetTimeouts(URLConnection connection, int socketTimeout)
		{
			connection.SetConnectTimeout(socketTimeout);
			connection.SetReadTimeout(socketTimeout);
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] argv)
		{
			CommandLine cliParser = new GnuParser().Parse(opts, argv);
			if (cliParser.HasOption("put"))
			{
				string path = cliParser.GetOptionValue("put");
				if (path != null && path.Length > 0)
				{
					if (cliParser.HasOption(EntityDataType))
					{
						PutTimelineDataInJSONFile(path, EntityDataType);
						return;
					}
					else
					{
						if (cliParser.HasOption(DomainDataType))
						{
							PutTimelineDataInJSONFile(path, DomainDataType);
							return;
						}
					}
				}
			}
			PrintUsage();
		}

		/// <summary>Put timeline data in a JSON file via command line.</summary>
		/// <param name="path">path to the timeline data JSON file</param>
		/// <param name="type">the type of the timeline data in the JSON file</param>
		private static void PutTimelineDataInJSONFile(string path, string type)
		{
			FilePath jsonFile = new FilePath(path);
			if (!jsonFile.Exists())
			{
				Log.Error("File [" + jsonFile.GetAbsolutePath() + "] doesn't exist");
				return;
			}
			ObjectMapper mapper = new ObjectMapper();
			YarnJacksonJaxbJsonProvider.ConfigObjectMapper(mapper);
			TimelineEntities entities = null;
			TimelineDomains domains = null;
			try
			{
				if (type.Equals(EntityDataType))
				{
					entities = mapper.ReadValue<TimelineEntities>(jsonFile);
				}
				else
				{
					if (type.Equals(DomainDataType))
					{
						domains = mapper.ReadValue<TimelineDomains>(jsonFile);
					}
				}
			}
			catch (Exception e)
			{
				Log.Error("Error when reading  " + e.Message);
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Error);
				return;
			}
			Configuration conf = new YarnConfiguration();
			TimelineClient client = TimelineClient.CreateTimelineClient();
			client.Init(conf);
			client.Start();
			try
			{
				if (UserGroupInformation.IsSecurityEnabled() && conf.GetBoolean(YarnConfiguration
					.TimelineServiceEnabled, false))
				{
					Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier> token = 
						client.GetDelegationToken(UserGroupInformation.GetCurrentUser().GetUserName());
					UserGroupInformation.GetCurrentUser().AddToken(token);
				}
				if (type.Equals(EntityDataType))
				{
					TimelinePutResponse response = client.PutEntities(Sharpen.Collections.ToArray(entities
						.GetEntities(), new TimelineEntity[entities.GetEntities().Count]));
					if (response.GetErrors().Count == 0)
					{
						Log.Info("Timeline entities are successfully put");
					}
					else
					{
						foreach (TimelinePutResponse.TimelinePutError error in response.GetErrors())
						{
							Log.Error("TimelineEntity [" + error.GetEntityType() + ":" + error.GetEntityId() 
								+ "] is not successfully put. Error code: " + error.GetErrorCode());
						}
					}
				}
				else
				{
					if (type.Equals(DomainDataType))
					{
						bool hasError = false;
						foreach (TimelineDomain domain in domains.GetDomains())
						{
							try
							{
								client.PutDomain(domain);
							}
							catch (Exception e)
							{
								Log.Error("Error when putting domain " + domain.GetId(), e);
								hasError = true;
							}
						}
						if (!hasError)
						{
							Log.Info("Timeline domains are successfully put");
						}
					}
				}
			}
			catch (RuntimeException e)
			{
				Log.Error("Error when putting the timeline data", e);
			}
			catch (Exception e)
			{
				Log.Error("Error when putting the timeline data", e);
			}
			finally
			{
				client.Stop();
			}
		}

		/// <summary>Helper function to print out usage</summary>
		private static void PrintUsage()
		{
			new HelpFormatter().PrintHelp("TimelineClient", opts);
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		public virtual UserGroupInformation GetUgi()
		{
			return authUgi;
		}
	}
}
