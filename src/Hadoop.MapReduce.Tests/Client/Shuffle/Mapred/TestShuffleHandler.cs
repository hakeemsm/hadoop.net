/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Task.Reduce;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Socket;
using Org.Jboss.Netty.Handler.Codec.Http;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestShuffleHandler
	{
		internal const long MiB = 1024 * 1024;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestShuffleHandler));

		internal class MockShuffleHandler : ShuffleHandler
		{
			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_106(conf);
			}

			private sealed class _Shuffle_106 : ShuffleHandler.Shuffle
			{
				public _Shuffle_106(Configuration baseArg1)
					: base(baseArg1)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ShuffleHandler.Shuffle.MapOutputInfo GetMapOutputInfo
					(string @base, string mapId, int reduce, string user)
				{
					// Do nothing.
					return null;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void PopulateHeaders(IList<string> mapIds, string jobId
					, string user, int reduce, HttpRequest request, HttpResponse response, bool keepAliveParam
					, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> infoMap)
				{
				}

				// Do nothing.
				/// <exception cref="System.IO.IOException"/>
				protected internal override ChannelFuture SendMapOutput(ChannelHandlerContext ctx
					, Org.Jboss.Netty.Channel.Channel ch, string user, string mapId, int reduce, ShuffleHandler.Shuffle.MapOutputInfo
					 info)
				{
					ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
					dob = new DataOutputBuffer();
					for (int i = 0; i < 100; ++i)
					{
						header.Write(dob);
					}
					return ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
				}
			}

			internal MockShuffleHandler(TestShuffleHandler _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestShuffleHandler _enclosing;
		}

		private class MockShuffleHandler2 : ShuffleHandler
		{
			internal bool socketKeepAlive = false;

			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_150(this, conf);
			}

			private sealed class _Shuffle_150 : ShuffleHandler.Shuffle
			{
				public _Shuffle_150(MockShuffleHandler2 _enclosing, Configuration baseArg1)
					: base(_enclosing, baseArg1)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
					SocketChannel channel = (SocketChannel)(ctx.GetChannel());
					this._enclosing.socketKeepAlive = ((SocketChannelConfig)channel.GetConfig()).IsKeepAlive
						();
				}

				private readonly MockShuffleHandler2 _enclosing;
			}

			protected internal virtual bool IsSocketKeepAlive()
			{
				return socketKeepAlive;
			}
		}

		/// <summary>
		/// Test the validation of ShuffleHandler's meta-data's serialization and
		/// de-serialization.
		/// </summary>
		/// <exception cref="System.Exception">exception</exception>
		public virtual void TestSerializeMeta()
		{
			NUnit.Framework.Assert.AreEqual(1, ShuffleHandler.DeserializeMetaData(ShuffleHandler
				.SerializeMetaData(1)));
			NUnit.Framework.Assert.AreEqual(-1, ShuffleHandler.DeserializeMetaData(ShuffleHandler
				.SerializeMetaData(-1)));
			NUnit.Framework.Assert.AreEqual(8080, ShuffleHandler.DeserializeMetaData(ShuffleHandler
				.SerializeMetaData(8080)));
		}

		/// <summary>Validate shuffle connection and input/output metrics.</summary>
		/// <exception cref="System.Exception">exception</exception>
		public virtual void TestShuffleMetrics()
		{
			MetricsSystem ms = new MetricsSystemImpl();
			ShuffleHandler sh = new ShuffleHandler(ms);
			ChannelFuture cf = MockitoMaker.Make(MockitoMaker.Stub<ChannelFuture>().Returning
				(true, false).from.IsSuccess());
			sh.metrics.shuffleConnections.Incr();
			sh.metrics.shuffleOutputBytes.Incr(1 * MiB);
			sh.metrics.shuffleConnections.Incr();
			sh.metrics.shuffleOutputBytes.Incr(2 * MiB);
			CheckShuffleMetrics(ms, 3 * MiB, 0, 0, 2);
			sh.metrics.OperationComplete(cf);
			sh.metrics.OperationComplete(cf);
			CheckShuffleMetrics(ms, 3 * MiB, 1, 1, 0);
		}

		internal static void CheckShuffleMetrics(MetricsSystem ms, long bytes, int failed
			, int succeeded, int connections)
		{
			MetricsSource source = ms.GetSource("ShuffleMetrics");
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source);
			MetricsAsserts.AssertCounter("ShuffleOutputBytes", bytes, rb);
			MetricsAsserts.AssertCounter("ShuffleOutputsFailed", failed, rb);
			MetricsAsserts.AssertCounter("ShuffleOutputsOK", succeeded, rb);
			MetricsAsserts.AssertGauge("ShuffleConnections", connections, rb);
		}

		/// <summary>Verify client prematurely closing a connection.</summary>
		/// <exception cref="System.Exception">exception.</exception>
		public virtual void TestClientClosesConnection()
		{
			AList<Exception> failures = new AList<Exception>(1);
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			ShuffleHandler shuffleHandler = new _ShuffleHandler_227(failures);
			// replace the shuffle handler with one stubbed for testing
			// Only set response headers and skip everything else
			// send some dummy value for content-length
			// send a shuffle header and a lot of data down the channel
			// to trigger a broken pipe
			shuffleHandler.Init(conf);
			shuffleHandler.Start();
			// simulate a reducer that closes early by reading a single shuffle header
			// then closing the connection
			Uri url = new Uri("http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
				.ShufflePortConfigKey) + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_1_0"
				);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
				);
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
				);
			conn.Connect();
			DataInputStream input = new DataInputStream(conn.GetInputStream());
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			NUnit.Framework.Assert.AreEqual("close", conn.GetHeaderField(HttpHeaders.Connection
				));
			ShuffleHeader header = new ShuffleHeader();
			header.ReadFields(input);
			input.Close();
			shuffleHandler.Stop();
			NUnit.Framework.Assert.IsTrue("sendError called when client closed connection", failures
				.Count == 0);
		}

		private sealed class _ShuffleHandler_227 : ShuffleHandler
		{
			public _ShuffleHandler_227(AList<Exception> failures)
			{
				this.failures = failures;
			}

			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_231(failures, conf);
			}

			private sealed class _Shuffle_231 : ShuffleHandler.Shuffle
			{
				public _Shuffle_231(AList<Exception> failures, Configuration baseArg1)
					: base(baseArg1)
				{
					this.failures = failures;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ShuffleHandler.Shuffle.MapOutputInfo GetMapOutputInfo
					(string @base, string mapId, int reduce, string user)
				{
					return null;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void PopulateHeaders(IList<string> mapIds, string jobId
					, string user, int reduce, HttpRequest request, HttpResponse response, bool keepAliveParam
					, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> infoMap)
				{
					base.SetResponseHeaders(response, keepAliveParam, 100);
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ChannelFuture SendMapOutput(ChannelHandlerContext ctx
					, Org.Jboss.Netty.Channel.Channel ch, string user, string mapId, int reduce, ShuffleHandler.Shuffle.MapOutputInfo
					 info)
				{
					ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
					dob = new DataOutputBuffer();
					for (int i = 0; i < 100000; ++i)
					{
						header.Write(dob);
					}
					return ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
				}

				protected internal override void SendError(ChannelHandlerContext ctx, HttpResponseStatus
					 status)
				{
					if (failures.Count == 0)
					{
						failures.AddItem(new Error());
						ctx.GetChannel().Close();
					}
				}

				protected internal override void SendError(ChannelHandlerContext ctx, string message
					, HttpResponseStatus status)
				{
					if (failures.Count == 0)
					{
						failures.AddItem(new Error());
						ctx.GetChannel().Close();
					}
				}

				private readonly AList<Exception> failures;
			}

			private readonly AList<Exception> failures;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestKeepAlive()
		{
			AList<Exception> failures = new AList<Exception>(1);
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetBoolean(ShuffleHandler.ShuffleConnectionKeepAliveEnabled, true);
			// try setting to -ve keep alive timeout.
			conf.SetInt(ShuffleHandler.ShuffleConnectionKeepAliveTimeOut, -100);
			ShuffleHandler shuffleHandler = new _ShuffleHandler_322(this, failures);
			// replace the shuffle handler with one stubbed for testing
			// Send some dummy data (populate content length details)
			// for testing purpose;
			// disable connectinKeepAliveEnabled if keepAliveParam is available
			// send a shuffle header and a lot of data down the channel
			// to trigger a broken pipe
			shuffleHandler.Init(conf);
			shuffleHandler.Start();
			string shuffleBaseURL = "http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
				.ShufflePortConfigKey);
			Uri url = new Uri(shuffleBaseURL + "/mapOutput?job=job_12345_1&reduce=1&" + "map=attempt_12345_1_m_1_0"
				);
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
				);
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
				);
			conn.Connect();
			DataInputStream input = new DataInputStream(conn.GetInputStream());
			NUnit.Framework.Assert.AreEqual(HttpHeaders.KeepAlive, conn.GetHeaderField(HttpHeaders
				.Connection));
			NUnit.Framework.Assert.AreEqual("timeout=1", conn.GetHeaderField(HttpHeaders.KeepAlive
				));
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			ShuffleHeader header = new ShuffleHeader();
			header.ReadFields(input);
			input.Close();
			// For keepAlive via URL
			url = new Uri(shuffleBaseURL + "/mapOutput?job=job_12345_1&reduce=1&" + "map=attempt_12345_1_m_1_0&keepAlive=true"
				);
			conn = (HttpURLConnection)url.OpenConnection();
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
				);
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
				);
			conn.Connect();
			input = new DataInputStream(conn.GetInputStream());
			NUnit.Framework.Assert.AreEqual(HttpHeaders.KeepAlive, conn.GetHeaderField(HttpHeaders
				.Connection));
			NUnit.Framework.Assert.AreEqual("timeout=1", conn.GetHeaderField(HttpHeaders.KeepAlive
				));
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, conn.GetResponseCode());
			header = new ShuffleHeader();
			header.ReadFields(input);
			input.Close();
		}

		private sealed class _ShuffleHandler_322 : ShuffleHandler
		{
			public _ShuffleHandler_322(TestShuffleHandler _enclosing, AList<Exception> failures
				)
			{
				this._enclosing = _enclosing;
				this.failures = failures;
			}

			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_326(this, failures, conf);
			}

			private sealed class _Shuffle_326 : ShuffleHandler.Shuffle
			{
				public _Shuffle_326(_ShuffleHandler_322 _enclosing, AList<Exception> failures, Configuration
					 baseArg1)
					: base(_enclosing, baseArg1)
				{
					this._enclosing = _enclosing;
					this.failures = failures;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ShuffleHandler.Shuffle.MapOutputInfo GetMapOutputInfo
					(string @base, string mapId, int reduce, string user)
				{
					return null;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void PopulateHeaders(IList<string> mapIds, string jobId
					, string user, int reduce, HttpRequest request, HttpResponse response, bool keepAliveParam
					, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> infoMap)
				{
					ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					dob = new DataOutputBuffer();
					for (int i = 0; i < 100000; ++i)
					{
						header.Write(dob);
					}
					long contentLength = dob.GetLength();
					if (keepAliveParam)
					{
						this._enclosing.connectionKeepAliveEnabled = false;
					}
					base.SetResponseHeaders(response, keepAliveParam, contentLength);
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ChannelFuture SendMapOutput(ChannelHandlerContext ctx
					, Org.Jboss.Netty.Channel.Channel ch, string user, string mapId, int reduce, ShuffleHandler.Shuffle.MapOutputInfo
					 info)
				{
					HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
						.Ok);
					ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
					dob = new DataOutputBuffer();
					for (int i = 0; i < 100000; ++i)
					{
						header.Write(dob);
					}
					return ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
				}

				protected internal override void SendError(ChannelHandlerContext ctx, HttpResponseStatus
					 status)
				{
					if (failures.Count == 0)
					{
						failures.AddItem(new Error());
						ctx.GetChannel().Close();
					}
				}

				protected internal override void SendError(ChannelHandlerContext ctx, string message
					, HttpResponseStatus status)
				{
					if (failures.Count == 0)
					{
						failures.AddItem(new Error());
						ctx.GetChannel().Close();
					}
				}

				private readonly _ShuffleHandler_322 _enclosing;

				private readonly AList<Exception> failures;
			}

			private readonly TestShuffleHandler _enclosing;

			private readonly AList<Exception> failures;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSocketKeepAlive()
		{
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetBoolean(ShuffleHandler.ShuffleConnectionKeepAliveEnabled, true);
			// try setting to -ve keep alive timeout.
			conf.SetInt(ShuffleHandler.ShuffleConnectionKeepAliveTimeOut, -100);
			HttpURLConnection conn = null;
			TestShuffleHandler.MockShuffleHandler2 shuffleHandler = new TestShuffleHandler.MockShuffleHandler2
				();
			try
			{
				shuffleHandler.Init(conf);
				shuffleHandler.Start();
				string shuffleBaseURL = "http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
					.ShufflePortConfigKey);
				Uri url = new Uri(shuffleBaseURL + "/mapOutput?job=job_12345_1&reduce=1&" + "map=attempt_12345_1_m_1_0"
					);
				conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
					);
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
					);
				conn.Connect();
				conn.GetInputStream();
				NUnit.Framework.Assert.IsTrue("socket should be set KEEP_ALIVE", shuffleHandler.IsSocketKeepAlive
					());
			}
			finally
			{
				if (conn != null)
				{
					conn.Disconnect();
				}
				shuffleHandler.Stop();
			}
		}

		/// <summary>
		/// simulate a reducer that sends an invalid shuffle-header - sometimes a wrong
		/// header_name and sometimes a wrong version
		/// </summary>
		/// <exception cref="System.Exception">exception</exception>
		public virtual void TestIncompatibleShuffleVersion()
		{
			int failureNum = 3;
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			ShuffleHandler shuffleHandler = new ShuffleHandler();
			shuffleHandler.Init(conf);
			shuffleHandler.Start();
			// simulate a reducer that closes early by reading a single shuffle header
			// then closing the connection
			Uri url = new Uri("http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
				.ShufflePortConfigKey) + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_1_0"
				);
			for (int i = 0; i < failureNum; ++i)
			{
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, i == 0 ? "mapreduce" : "other"
					);
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, i == 1 ? "1.0.0" : "1.0.1"
					);
				conn.Connect();
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpBadRequest, conn.GetResponseCode
					());
			}
			shuffleHandler.Stop();
			shuffleHandler.Close();
		}

		/// <summary>Validate the limit on number of shuffle connections.</summary>
		/// <exception cref="System.Exception">exception</exception>
		public virtual void TestMaxConnections()
		{
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetInt(ShuffleHandler.MaxShuffleConnections, 3);
			ShuffleHandler shuffleHandler = new _ShuffleHandler_531();
			// replace the shuffle handler with one stubbed for testing
			// Do nothing.
			// Do nothing.
			// Do nothing.
			// send a shuffle header and a lot of data down the channel
			// to trigger a broken pipe
			shuffleHandler.Init(conf);
			shuffleHandler.Start();
			// setup connections
			int connAttempts = 3;
			HttpURLConnection[] conns = new HttpURLConnection[connAttempts];
			for (int i = 0; i < connAttempts; i++)
			{
				string URLstring = "http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
					.ShufflePortConfigKey) + "/mapOutput?job=job_12345_1&reduce=1&map=attempt_12345_1_m_"
					 + i + "_0";
				Uri url = new Uri(URLstring);
				conns[i] = (HttpURLConnection)url.OpenConnection();
				conns[i].SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
					);
				conns[i].SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
					);
			}
			// Try to open numerous connections
			for (int i_1 = 0; i_1 < connAttempts; i_1++)
			{
				conns[i_1].Connect();
			}
			//Ensure first connections are okay
			conns[0].GetInputStream();
			int rc = conns[0].GetResponseCode();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
			conns[1].GetInputStream();
			rc = conns[1].GetResponseCode();
			NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
			// This connection should be closed because it to above the limit
			try
			{
				conns[2].GetInputStream();
				rc = conns[2].GetResponseCode();
				NUnit.Framework.Assert.Fail("Expected a SocketException");
			}
			catch (SocketException)
			{
				Log.Info("Expected - connection should not be open");
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Expected a SocketException");
			}
			shuffleHandler.Stop();
		}

		private sealed class _ShuffleHandler_531 : ShuffleHandler
		{
			public _ShuffleHandler_531()
			{
			}

			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_535(conf);
			}

			private sealed class _Shuffle_535 : ShuffleHandler.Shuffle
			{
				public _Shuffle_535(Configuration baseArg1)
					: base(baseArg1)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ShuffleHandler.Shuffle.MapOutputInfo GetMapOutputInfo
					(string @base, string mapId, int reduce, string user)
				{
					return null;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void PopulateHeaders(IList<string> mapIds, string jobId
					, string user, int reduce, HttpRequest request, HttpResponse response, bool keepAliveParam
					, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> infoMap)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ChannelFuture SendMapOutput(ChannelHandlerContext ctx
					, Org.Jboss.Netty.Channel.Channel ch, string user, string mapId, int reduce, ShuffleHandler.Shuffle.MapOutputInfo
					 info)
				{
					ShuffleHeader header = new ShuffleHeader("dummy_header", 5678, 5678, 1);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
					dob = new DataOutputBuffer();
					for (int i = 0; i < 100000; ++i)
					{
						header.Write(dob);
					}
					return ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
				}
			}
		}

		/// <summary>Validate the ownership of the map-output files being pulled in.</summary>
		/// <remarks>
		/// Validate the ownership of the map-output files being pulled in. The
		/// local-file-system owner of the file should match the user component in the
		/// </remarks>
		/// <exception cref="System.Exception">exception</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMapFileAccess()
		{
			// This will run only in NativeIO is enabled as SecureIOUtils need it
			Assume.AssumeTrue(NativeIO.IsAvailable());
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetInt(ShuffleHandler.MaxShuffleConnections, 3);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			FilePath absLogDir = new FilePath("target", typeof(TestShuffleHandler).Name + "LocDir"
				).GetAbsoluteFile();
			conf.Set(YarnConfiguration.NmLocalDirs, absLogDir.GetAbsolutePath());
			ApplicationId appId = ApplicationId.NewInstance(12345, 1);
			Log.Info(appId.ToString());
			string appAttemptId = "attempt_12345_1_m_1_0";
			string user = "randomUser";
			string reducerId = "0";
			IList<FilePath> fileMap = new AList<FilePath>();
			CreateShuffleHandlerFiles(absLogDir, user, appId.ToString(), appAttemptId, conf, 
				fileMap);
			ShuffleHandler shuffleHandler = new _ShuffleHandler_651();
			// replace the shuffle handler with one stubbed for testing
			// Do nothing.
			shuffleHandler.Init(conf);
			try
			{
				shuffleHandler.Start();
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
				outputBuffer.Reset();
				Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = new Org.Apache.Hadoop.Security.Token.Token
					<JobTokenIdentifier>(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
					("password"), new Text(user), new Text("shuffleService"));
				jt.Write(outputBuffer);
				shuffleHandler.InitializeApplication(new ApplicationInitializationContext(user, appId
					, ByteBuffer.Wrap(outputBuffer.GetData(), 0, outputBuffer.GetLength())));
				Uri url = new Uri("http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
					.ShufflePortConfigKey) + "/mapOutput?job=job_12345_0001&reduce=" + reducerId + "&map=attempt_12345_1_m_1_0"
					);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
					);
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
					);
				conn.Connect();
				byte[] byteArr = new byte[10000];
				try
				{
					DataInputStream @is = new DataInputStream(conn.GetInputStream());
					@is.ReadFully(byteArr);
				}
				catch (EOFException)
				{
				}
				// ignore
				// Retrieve file owner name
				FileInputStream is_1 = new FileInputStream(fileMap[0]);
				string owner = NativeIO.POSIX.GetFstat(is_1.GetFD()).GetOwner();
				is_1.Close();
				string message = "Owner '" + owner + "' for path " + fileMap[0].GetAbsolutePath()
					 + " did not match expected owner '" + user + "'";
				NUnit.Framework.Assert.IsTrue((Sharpen.Runtime.GetStringForBytes(byteArr)).Contains
					(message));
			}
			finally
			{
				shuffleHandler.Stop();
				FileUtil.FullyDelete(absLogDir);
			}
		}

		private sealed class _ShuffleHandler_651 : ShuffleHandler
		{
			public _ShuffleHandler_651()
			{
			}

			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_656(conf);
			}

			private sealed class _Shuffle_656 : ShuffleHandler.Shuffle
			{
				public _Shuffle_656(Configuration baseArg1)
					: base(baseArg1)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateShuffleHandlerFiles(FilePath logDir, string user, string
			 appId, string appAttemptId, Configuration conf, IList<FilePath> fileMap)
		{
			string attemptDir = StringUtils.Join(Path.Separator, Arrays.AsList(new string[] { 
				logDir.GetAbsolutePath(), ContainerLocalizer.Usercache, user, ContainerLocalizer
				.Appcache, appId, "output", appAttemptId }));
			FilePath appAttemptDir = new FilePath(attemptDir);
			appAttemptDir.Mkdirs();
			System.Console.Out.WriteLine(appAttemptDir.GetAbsolutePath());
			FilePath indexFile = new FilePath(appAttemptDir, "file.out.index");
			fileMap.AddItem(indexFile);
			CreateIndexFile(indexFile, conf);
			FilePath mapOutputFile = new FilePath(appAttemptDir, "file.out");
			fileMap.AddItem(mapOutputFile);
			CreateMapOutputFile(mapOutputFile, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateMapOutputFile(FilePath mapOutputFile, Configuration conf
			)
		{
			FileOutputStream @out = new FileOutputStream(mapOutputFile);
			@out.Write(Sharpen.Runtime.GetBytesForString("Creating new dummy map output file. Used only for testing"
				));
			@out.Flush();
			@out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateIndexFile(FilePath indexFile, Configuration conf)
		{
			if (indexFile.Exists())
			{
				System.Console.Out.WriteLine("Deleting existing file");
				indexFile.Delete();
			}
			indexFile.CreateNewFile();
			FSDataOutputStream output = FileSystem.GetLocal(conf).GetRaw().Append(new Path(indexFile
				.GetAbsolutePath()));
			Checksum crc = new PureJavaCrc32();
			crc.Reset();
			CheckedOutputStream chk = new CheckedOutputStream(output, crc);
			string msg = "Writing new index file. This file will be used only " + "for the testing.";
			chk.Write(Arrays.CopyOf(Sharpen.Runtime.GetBytesForString(msg), MapTask.MapOutputIndexRecordLength
				));
			output.WriteLong(chk.GetChecksum().GetValue());
			output.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(12345, 1);
			JobID jobId = JobID.Downgrade(TypeConverter.FromYarn(appId));
			FilePath tmpDir = new FilePath(Runtime.GetProperty("test.build.data", Runtime.GetProperty
				("java.io.tmpdir")), typeof(TestShuffleHandler).FullName);
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetInt(ShuffleHandler.MaxShuffleConnections, 3);
			ShuffleHandler shuffle = new ShuffleHandler();
			// emulate aux services startup with recovery enabled
			shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
			tmpDir.Mkdirs();
			try
			{
				shuffle.Init(conf);
				shuffle.Start();
				// setup a shuffle token for an application
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
				outputBuffer.Reset();
				Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = new Org.Apache.Hadoop.Security.Token.Token
					<JobTokenIdentifier>(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
					("password"), new Text(user), new Text("shuffleService"));
				jt.Write(outputBuffer);
				shuffle.InitializeApplication(new ApplicationInitializationContext(user, appId, ByteBuffer
					.Wrap(outputBuffer.GetData(), 0, outputBuffer.GetLength())));
				// verify we are authorized to shuffle
				int rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
				// emulate shuffle handler restart
				shuffle.Close();
				shuffle = new ShuffleHandler();
				shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
				shuffle.Init(conf);
				shuffle.Start();
				// verify we are still authorized to shuffle to the old application
				rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
				// shutdown app and verify access is lost
				shuffle.StopApplication(new ApplicationTerminationContext(appId));
				rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, rc);
				// emulate shuffle handler restart
				shuffle.Close();
				shuffle = new ShuffleHandler();
				shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
				shuffle.Init(conf);
				shuffle.Start();
				// verify we still don't have access
				rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpUnauthorized, rc);
			}
			finally
			{
				if (shuffle != null)
				{
					shuffle.Close();
				}
				FileUtil.FullyDelete(tmpDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoveryFromOtherVersions()
		{
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(12345, 1);
			FilePath tmpDir = new FilePath(Runtime.GetProperty("test.build.data", Runtime.GetProperty
				("java.io.tmpdir")), typeof(TestShuffleHandler).FullName);
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetInt(ShuffleHandler.MaxShuffleConnections, 3);
			ShuffleHandler shuffle = new ShuffleHandler();
			// emulate aux services startup with recovery enabled
			shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
			tmpDir.Mkdirs();
			try
			{
				shuffle.Init(conf);
				shuffle.Start();
				// setup a shuffle token for an application
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
				outputBuffer.Reset();
				Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = new Org.Apache.Hadoop.Security.Token.Token
					<JobTokenIdentifier>(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
					("password"), new Text(user), new Text("shuffleService"));
				jt.Write(outputBuffer);
				shuffle.InitializeApplication(new ApplicationInitializationContext(user, appId, ByteBuffer
					.Wrap(outputBuffer.GetData(), 0, outputBuffer.GetLength())));
				// verify we are authorized to shuffle
				int rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
				// emulate shuffle handler restart
				shuffle.Close();
				shuffle = new ShuffleHandler();
				shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
				shuffle.Init(conf);
				shuffle.Start();
				// verify we are still authorized to shuffle to the old application
				rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
				Version version = Version.NewInstance(1, 0);
				NUnit.Framework.Assert.AreEqual(version, shuffle.GetCurrentVersion());
				// emulate shuffle handler restart with compatible version
				Version version11 = Version.NewInstance(1, 1);
				// update version info before close shuffle
				shuffle.StoreVersion(version11);
				NUnit.Framework.Assert.AreEqual(version11, shuffle.LoadVersion());
				shuffle.Close();
				shuffle = new ShuffleHandler();
				shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
				shuffle.Init(conf);
				shuffle.Start();
				// shuffle version will be override by CURRENT_VERSION_INFO after restart
				// successfully.
				NUnit.Framework.Assert.AreEqual(version, shuffle.LoadVersion());
				// verify we are still authorized to shuffle to the old application
				rc = GetShuffleResponseCode(shuffle, jt);
				NUnit.Framework.Assert.AreEqual(HttpURLConnection.HttpOk, rc);
				// emulate shuffle handler restart with incompatible version
				Version version21 = Version.NewInstance(2, 1);
				shuffle.StoreVersion(version21);
				NUnit.Framework.Assert.AreEqual(version21, shuffle.LoadVersion());
				shuffle.Close();
				shuffle = new ShuffleHandler();
				shuffle.SetRecoveryPath(new Path(tmpDir.ToString()));
				shuffle.Init(conf);
				try
				{
					shuffle.Start();
					NUnit.Framework.Assert.Fail("Incompatible version, should expect fail here.");
				}
				catch (ServiceStateException e)
				{
					NUnit.Framework.Assert.IsTrue("Exception message mismatch", e.Message.Contains("Incompatible version for state DB schema:"
						));
				}
			}
			finally
			{
				if (shuffle != null)
				{
					shuffle.Close();
				}
				FileUtil.FullyDelete(tmpDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static int GetShuffleResponseCode(ShuffleHandler shuffle, Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> jt)
		{
			Uri url = new Uri("http://127.0.0.1:" + shuffle.GetConfig().Get(ShuffleHandler.ShufflePortConfigKey
				) + "/mapOutput?job=job_12345_0001&reduce=0&map=attempt_12345_1_m_1_0");
			HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
			string encHash = SecureShuffleUtils.HashFromString(SecureShuffleUtils.BuildMsgFrom
				(url), JobTokenSecretManager.CreateSecretKey(jt.GetPassword()));
			conn.AddRequestProperty(SecureShuffleUtils.HttpHeaderUrlHash, encHash);
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
				);
			conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
				);
			conn.Connect();
			int rc = conn.GetResponseCode();
			conn.Disconnect();
			return rc;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetMapOutputInfo()
		{
			AList<Exception> failures = new AList<Exception>(1);
			Configuration conf = new Configuration();
			conf.SetInt(ShuffleHandler.ShufflePortConfigKey, 0);
			conf.SetInt(ShuffleHandler.MaxShuffleConnections, 3);
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
			UserGroupInformation.SetConfiguration(conf);
			FilePath absLogDir = new FilePath("target", typeof(TestShuffleHandler).Name + "LocDir"
				).GetAbsoluteFile();
			conf.Set(YarnConfiguration.NmLocalDirs, absLogDir.GetAbsolutePath());
			ApplicationId appId = ApplicationId.NewInstance(12345, 1);
			string appAttemptId = "attempt_12345_1_m_1_0";
			string user = "randomUser";
			string reducerId = "0";
			IList<FilePath> fileMap = new AList<FilePath>();
			CreateShuffleHandlerFiles(absLogDir, user, appId.ToString(), appAttemptId, conf, 
				fileMap);
			ShuffleHandler shuffleHandler = new _ShuffleHandler_961(failures);
			// replace the shuffle handler with one stubbed for testing
			// Only set response headers and skip everything else
			// send some dummy value for content-length
			// Do nothing.
			// send a shuffle header
			shuffleHandler.Init(conf);
			try
			{
				shuffleHandler.Start();
				DataOutputBuffer outputBuffer = new DataOutputBuffer();
				outputBuffer.Reset();
				Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = new Org.Apache.Hadoop.Security.Token.Token
					<JobTokenIdentifier>(Sharpen.Runtime.GetBytesForString("identifier"), Sharpen.Runtime.GetBytesForString
					("password"), new Text(user), new Text("shuffleService"));
				jt.Write(outputBuffer);
				shuffleHandler.InitializeApplication(new ApplicationInitializationContext(user, appId
					, ByteBuffer.Wrap(outputBuffer.GetData(), 0, outputBuffer.GetLength())));
				Uri url = new Uri("http://127.0.0.1:" + shuffleHandler.GetConfig().Get(ShuffleHandler
					.ShufflePortConfigKey) + "/mapOutput?job=job_12345_0001&reduce=" + reducerId + "&map=attempt_12345_1_m_1_0"
					);
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
					);
				conn.SetRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
					);
				conn.Connect();
				try
				{
					DataInputStream @is = new DataInputStream(conn.GetInputStream());
					ShuffleHeader header = new ShuffleHeader();
					header.ReadFields(@is);
					@is.Close();
				}
				catch (EOFException)
				{
				}
				// ignore
				NUnit.Framework.Assert.AreEqual("sendError called due to shuffle error", 0, failures
					.Count);
			}
			finally
			{
				shuffleHandler.Stop();
				FileUtil.FullyDelete(absLogDir);
			}
		}

		private sealed class _ShuffleHandler_961 : ShuffleHandler
		{
			public _ShuffleHandler_961(AList<Exception> failures)
			{
				this.failures = failures;
			}

			protected internal override ShuffleHandler.Shuffle GetShuffle(Configuration conf)
			{
				return new _Shuffle_965(failures, conf);
			}

			private sealed class _Shuffle_965 : ShuffleHandler.Shuffle
			{
				public _Shuffle_965(AList<Exception> failures, Configuration baseArg1)
					: base(baseArg1)
				{
					this.failures = failures;
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void PopulateHeaders(IList<string> mapIds, string outputBaseStr
					, string user, int reduce, HttpRequest request, HttpResponse response, bool keepAliveParam
					, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> infoMap)
				{
					base.SetResponseHeaders(response, keepAliveParam, 100);
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override void VerifyRequest(string appid, ChannelHandlerContext
					 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
				{
				}

				protected internal override void SendError(ChannelHandlerContext ctx, string message
					, HttpResponseStatus status)
				{
					if (failures.Count == 0)
					{
						failures.AddItem(new Error(message));
						ctx.GetChannel().Close();
					}
				}

				/// <exception cref="System.IO.IOException"/>
				protected internal override ChannelFuture SendMapOutput(ChannelHandlerContext ctx
					, Org.Jboss.Netty.Channel.Channel ch, string user, string mapId, int reduce, ShuffleHandler.Shuffle.MapOutputInfo
					 info)
				{
					ShuffleHeader header = new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					return ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
				}

				private readonly AList<Exception> failures;
			}

			private readonly AList<Exception> failures;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSendMapCount()
		{
			IList<ShuffleHandler.ReduceMapFileCount> listenerList = new AList<ShuffleHandler.ReduceMapFileCount
				>();
			ChannelHandlerContext mockCtx = Org.Mockito.Mockito.Mock<ChannelHandlerContext>();
			MessageEvent mockEvt = Org.Mockito.Mockito.Mock<MessageEvent>();
			Org.Jboss.Netty.Channel.Channel mockCh = Org.Mockito.Mockito.Mock<AbstractChannel
				>();
			// Mock HttpRequest and ChannelFuture
			HttpRequest mockHttpRequest = CreateMockHttpRequest();
			ChannelFuture mockFuture = CreateMockChannelFuture(mockCh, listenerList);
			// Mock Netty Channel Context and Channel behavior
			Org.Mockito.Mockito.DoReturn(mockCh).When(mockCtx).GetChannel();
			Org.Mockito.Mockito.When(mockCtx.GetChannel()).ThenReturn(mockCh);
			Org.Mockito.Mockito.DoReturn(mockFuture).When(mockCh).Write(Org.Mockito.Mockito.Any
				<object>());
			Org.Mockito.Mockito.When(mockCh.Write(typeof(object))).ThenReturn(mockFuture);
			//Mock MessageEvent behavior
			Org.Mockito.Mockito.DoReturn(mockCh).When(mockEvt).GetChannel();
			Org.Mockito.Mockito.When(mockEvt.GetChannel()).ThenReturn(mockCh);
			Org.Mockito.Mockito.DoReturn(mockHttpRequest).When(mockEvt).GetMessage();
			ShuffleHandler sh = new TestShuffleHandler.MockShuffleHandler(this);
			Configuration conf = new Configuration();
			sh.Init(conf);
			sh.Start();
			int maxOpenFiles = conf.GetInt(ShuffleHandler.ShuffleMaxSessionOpenFiles, ShuffleHandler
				.DefaultShuffleMaxSessionOpenFiles);
			sh.GetShuffle(conf).MessageReceived(mockCtx, mockEvt);
			NUnit.Framework.Assert.IsTrue("Number of Open files should not exceed the configured "
				 + "value!-Not Expected", listenerList.Count <= maxOpenFiles);
			while (!listenerList.IsEmpty())
			{
				listenerList.Remove(0).OperationComplete(mockFuture);
				NUnit.Framework.Assert.IsTrue("Number of Open files should not exceed the configured "
					 + "value!-Not Expected", listenerList.Count <= maxOpenFiles);
			}
			sh.Close();
		}

		public virtual ChannelFuture CreateMockChannelFuture(Org.Jboss.Netty.Channel.Channel
			 mockCh, IList<ShuffleHandler.ReduceMapFileCount> listenerList)
		{
			ChannelFuture mockFuture = Org.Mockito.Mockito.Mock<ChannelFuture>();
			Org.Mockito.Mockito.When(mockFuture.GetChannel()).ThenReturn(mockCh);
			Org.Mockito.Mockito.DoReturn(true).When(mockFuture).IsSuccess();
			Org.Mockito.Mockito.DoAnswer(new _Answer_1096(listenerList)).When(mockFuture).AddListener
				(Org.Mockito.Mockito.Any<ShuffleHandler.ReduceMapFileCount>());
			//Add ReduceMapFileCount listener to a list
			return mockFuture;
		}

		private sealed class _Answer_1096 : Answer
		{
			public _Answer_1096(IList<ShuffleHandler.ReduceMapFileCount> listenerList)
			{
				this.listenerList = listenerList;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				if (invocation.GetArguments()[0].GetType() == typeof(ShuffleHandler.ReduceMapFileCount
					))
				{
					listenerList.AddItem((ShuffleHandler.ReduceMapFileCount)invocation.GetArguments()
						[0]);
				}
				return null;
			}

			private readonly IList<ShuffleHandler.ReduceMapFileCount> listenerList;
		}

		public virtual HttpRequest CreateMockHttpRequest()
		{
			HttpRequest mockHttpRequest = Org.Mockito.Mockito.Mock<HttpRequest>();
			Org.Mockito.Mockito.DoReturn(HttpMethod.Get).When(mockHttpRequest).GetMethod();
			Org.Mockito.Mockito.DoAnswer(new _Answer_1114()).When(mockHttpRequest).GetUri();
			return mockHttpRequest;
		}

		private sealed class _Answer_1114 : Answer
		{
			public _Answer_1114()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocation)
			{
				string uri = "/mapOutput?job=job_12345_1&reduce=1";
				for (int i = 0; i < 100; i++)
				{
					uri = uri.Concat("&map=attempt_12345_1_m_" + i + "_0");
				}
				return uri;
			}
		}
	}
}
