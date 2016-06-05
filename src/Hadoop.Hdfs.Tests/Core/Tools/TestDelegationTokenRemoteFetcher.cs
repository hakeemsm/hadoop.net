using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Org.Jboss.Netty.Bootstrap;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Socket.Nio;
using Org.Jboss.Netty.Handler.Codec.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Tools
{
	public class TestDelegationTokenRemoteFetcher
	{
		private static readonly Logger Log = Logger.GetLogger(typeof(TestDelegationTokenRemoteFetcher
			));

		private const string ExpDate = "124123512361236";

		private const string tokenFile = "http.file.dta";

		private static readonly URLConnectionFactory connectionFactory = URLConnectionFactory
			.DefaultSystemConnectionFactory;

		private int httpPort;

		private URI serviceUrl;

		private FileSystem fileSys;

		private Configuration conf;

		private ServerBootstrap bootstrap;

		private Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> testToken;

		private volatile Exception assertionError;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Init()
		{
			conf = new Configuration();
			fileSys = FileSystem.GetLocal(conf);
			httpPort = NetUtils.GetFreeSocketPort();
			serviceUrl = new URI("http://localhost:" + httpPort);
			testToken = CreateToken(serviceUrl);
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Clean()
		{
			if (fileSys != null)
			{
				fileSys.Delete(new Path(tokenFile), true);
			}
			if (bootstrap != null)
			{
				bootstrap.ReleaseExternalResources();
			}
		}

		/// <summary>try to fetch token without http server with IOException</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenFetchFail()
		{
			try
			{
				DelegationTokenFetcher.Main(new string[] { "-webservice=" + serviceUrl, tokenFile
					 });
				NUnit.Framework.Assert.Fail("Token fetcher shouldn't start in absense of NN");
			}
			catch (IOException)
			{
			}
		}

		/// <summary>try to fetch token without http server with IOException</summary>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestTokenRenewFail()
		{
			try
			{
				DelegationTokenFetcher.RenewDelegationToken(connectionFactory, serviceUrl, testToken
					);
				NUnit.Framework.Assert.Fail("Token fetcher shouldn't be able to renew tokens in absense of NN"
					);
			}
			catch (IOException)
			{
			}
		}

		/// <summary>try cancel token without http server with IOException</summary>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void ExpectedTokenCancelFail()
		{
			try
			{
				DelegationTokenFetcher.CancelDelegationToken(connectionFactory, serviceUrl, testToken
					);
				NUnit.Framework.Assert.Fail("Token fetcher shouldn't be able to cancel tokens in absense of NN"
					);
			}
			catch (IOException)
			{
			}
		}

		/// <summary>try fetch token and get http response with error</summary>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void ExpectedTokenRenewErrorHttpResponse()
		{
			bootstrap = StartHttpServer(httpPort, testToken, serviceUrl);
			try
			{
				DelegationTokenFetcher.RenewDelegationToken(connectionFactory, new URI(serviceUrl
					.ToString() + "/exception"), CreateToken(serviceUrl));
				NUnit.Framework.Assert.Fail("Token fetcher shouldn't be able to renew tokens using an invalid"
					 + " NN URL");
			}
			catch (IOException)
			{
			}
			if (assertionError != null)
			{
				throw assertionError;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestCancelTokenFromHttp()
		{
			bootstrap = StartHttpServer(httpPort, testToken, serviceUrl);
			DelegationTokenFetcher.CancelDelegationToken(connectionFactory, serviceUrl, testToken
				);
			if (assertionError != null)
			{
				throw assertionError;
			}
		}

		/// <summary>Call renew token using http server return new expiration time</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.FormatException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.Authentication.Client.AuthenticationException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void TestRenewTokenFromHttp()
		{
			bootstrap = StartHttpServer(httpPort, testToken, serviceUrl);
			NUnit.Framework.Assert.IsTrue("testRenewTokenFromHttp error", long.Parse(ExpDate)
				 == DelegationTokenFetcher.RenewDelegationToken(connectionFactory, serviceUrl, testToken
				));
			if (assertionError != null)
			{
				throw assertionError;
			}
		}

		/// <summary>Call fetch token using http server</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void ExpectedTokenIsRetrievedFromHttp()
		{
			bootstrap = StartHttpServer(httpPort, testToken, serviceUrl);
			DelegationTokenFetcher.Main(new string[] { "-webservice=" + serviceUrl, tokenFile
				 });
			Path p = new Path(fileSys.GetWorkingDirectory(), tokenFile);
			Credentials creds = Credentials.ReadTokenStorageFile(p, conf);
			IEnumerator<Org.Apache.Hadoop.Security.Token.Token<object>> itr = creds.GetAllTokens
				().GetEnumerator();
			NUnit.Framework.Assert.IsTrue("token not exist error", itr.HasNext());
			Org.Apache.Hadoop.Security.Token.Token<object> fetchedToken = itr.Next();
			Assert.AssertArrayEquals("token wrong identifier error", testToken.GetIdentifier(
				), fetchedToken.GetIdentifier());
			Assert.AssertArrayEquals("token wrong password error", testToken.GetPassword(), fetchedToken
				.GetPassword());
			if (assertionError != null)
			{
				throw assertionError;
			}
		}

		private static Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> 
			CreateToken(URI serviceUri)
		{
			byte[] pw = Sharpen.Runtime.GetBytesForString("hadoop");
			byte[] ident = new DelegationTokenIdentifier(new Text("owner"), new Text("renewer"
				), new Text("realuser")).GetBytes();
			Text service = new Text(serviceUri.ToString());
			return new Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>(ident
				, pw, HftpFileSystem.TokenKind, service);
		}

		private interface Handler
		{
			/// <exception cref="System.IO.IOException"/>
			void Handle(Org.Jboss.Netty.Channel.Channel channel, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, string serviceUrl);
		}

		private class FetchHandler : TestDelegationTokenRemoteFetcher.Handler
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Handle(Org.Jboss.Netty.Channel.Channel channel, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, string serviceUrl)
			{
				NUnit.Framework.Assert.AreEqual(this._enclosing.testToken, token);
				Credentials creds = new Credentials();
				creds.AddToken(new Text(serviceUrl), token);
				DataOutputBuffer @out = new DataOutputBuffer();
				creds.Write(@out);
				int fileLength = @out.GetData().Length;
				ChannelBuffer cbuffer = ChannelBuffers.Buffer(fileLength);
				cbuffer.WriteBytes(@out.GetData());
				HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
					.Ok);
				response.SetHeader(HttpHeaders.Names.ContentLength, fileLength.ToString());
				response.SetContent(cbuffer);
				channel.Write(response).AddListener(ChannelFutureListener.Close);
			}

			internal FetchHandler(TestDelegationTokenRemoteFetcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationTokenRemoteFetcher _enclosing;
		}

		private class RenewHandler : TestDelegationTokenRemoteFetcher.Handler
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Handle(Org.Jboss.Netty.Channel.Channel channel, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, string serviceUrl)
			{
				NUnit.Framework.Assert.AreEqual(this._enclosing.testToken, token);
				byte[] bytes = Sharpen.Runtime.GetBytesForString(TestDelegationTokenRemoteFetcher
					.ExpDate);
				ChannelBuffer cbuffer = ChannelBuffers.Buffer(bytes.Length);
				cbuffer.WriteBytes(bytes);
				HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
					.Ok);
				response.SetHeader(HttpHeaders.Names.ContentLength, bytes.Length.ToString());
				response.SetContent(cbuffer);
				channel.Write(response).AddListener(ChannelFutureListener.Close);
			}

			internal RenewHandler(TestDelegationTokenRemoteFetcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationTokenRemoteFetcher _enclosing;
		}

		private class ExceptionHandler : TestDelegationTokenRemoteFetcher.Handler
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Handle(Org.Jboss.Netty.Channel.Channel channel, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, string serviceUrl)
			{
				NUnit.Framework.Assert.AreEqual(this._enclosing.testToken, token);
				HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
					.MethodNotAllowed);
				channel.Write(response).AddListener(ChannelFutureListener.Close);
			}

			internal ExceptionHandler(TestDelegationTokenRemoteFetcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationTokenRemoteFetcher _enclosing;
		}

		private class CancelHandler : TestDelegationTokenRemoteFetcher.Handler
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void Handle(Org.Jboss.Netty.Channel.Channel channel, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, string serviceUrl)
			{
				NUnit.Framework.Assert.AreEqual(this._enclosing.testToken, token);
				HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
					.Ok);
				channel.Write(response).AddListener(ChannelFutureListener.Close);
			}

			internal CancelHandler(TestDelegationTokenRemoteFetcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDelegationTokenRemoteFetcher _enclosing;
		}

		private sealed class CredentialsLogicHandler : SimpleChannelUpstreamHandler
		{
			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;

			private readonly string serviceUrl;

			private readonly ImmutableMap<string, TestDelegationTokenRemoteFetcher.Handler> routes;

			public CredentialsLogicHandler(TestDelegationTokenRemoteFetcher _enclosing, Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token, string serviceUrl)
			{
				this._enclosing = _enclosing;
				routes = ImmutableMap.Of("/exception", new TestDelegationTokenRemoteFetcher.ExceptionHandler
					(this), "/cancelDelegationToken", new TestDelegationTokenRemoteFetcher.CancelHandler
					(this), "/getDelegationToken", new TestDelegationTokenRemoteFetcher.FetchHandler
					(this), "/renewDelegationToken", new TestDelegationTokenRemoteFetcher.RenewHandler
					(this));
				this.token = token;
				this.serviceUrl = serviceUrl;
			}

			/// <exception cref="System.Exception"/>
			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
			{
				HttpRequest request = (HttpRequest)e.GetMessage();
				if (request.GetMethod() == HttpMethod.Options)
				{
					// Mimic SPNEGO authentication
					HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
						.Ok);
					response.AddHeader("Set-Cookie", "hadoop-auth=1234");
					e.GetChannel().Write(response).AddListener(ChannelFutureListener.Close);
				}
				else
				{
					if (request.GetMethod() != HttpMethod.Get)
					{
						e.GetChannel().Close();
					}
				}
				UnmodifiableIterator<KeyValuePair<string, TestDelegationTokenRemoteFetcher.Handler
					>> iter = this.routes.GetEnumerator();
				while (iter.HasNext())
				{
					KeyValuePair<string, TestDelegationTokenRemoteFetcher.Handler> entry = iter.Next(
						);
					if (request.GetUri().Contains(entry.Key))
					{
						TestDelegationTokenRemoteFetcher.Handler handler = entry.Value;
						try
						{
							handler.Handle(e.GetChannel(), this.token, this.serviceUrl);
						}
						catch (Exception ee)
						{
							this._enclosing.assertionError = ee;
							HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
								.BadRequest);
							response.SetContent(ChannelBuffers.CopiedBuffer(ee.Message, Encoding.Default));
							e.GetChannel().Write(response).AddListener(ChannelFutureListener.Close);
						}
						return;
					}
				}
			}

			/// <exception cref="System.Exception"/>
			public override void ExceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			{
				Org.Jboss.Netty.Channel.Channel ch = e.GetChannel();
				Exception cause = e.GetCause();
				if (TestDelegationTokenRemoteFetcher.Log.IsDebugEnabled())
				{
					TestDelegationTokenRemoteFetcher.Log.Debug(cause.Message);
				}
				ch.Close().AddListener(ChannelFutureListener.Close);
			}

			private readonly TestDelegationTokenRemoteFetcher _enclosing;
		}

		private ServerBootstrap StartHttpServer(int port, Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token, URI url)
		{
			ServerBootstrap bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory
				(Executors.NewCachedThreadPool(), Executors.NewCachedThreadPool()));
			bootstrap.SetPipelineFactory(new _ChannelPipelineFactory_362(token, url));
			bootstrap.Bind(new IPEndPoint("localhost", port));
			return bootstrap;
		}

		private sealed class _ChannelPipelineFactory_362 : ChannelPipelineFactory
		{
			public _ChannelPipelineFactory_362(Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token, URI url)
			{
				this.token = token;
				this.url = url;
			}

			/// <exception cref="System.Exception"/>
			public ChannelPipeline GetPipeline()
			{
				return Channels.Pipeline(new HttpRequestDecoder(), new HttpChunkAggregator(65536)
					, new HttpResponseEncoder(), new TestDelegationTokenRemoteFetcher.CredentialsLogicHandler
					(this, token, url.ToString()));
			}

			private readonly Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
				> token;

			private readonly URI url;
		}
	}
}
