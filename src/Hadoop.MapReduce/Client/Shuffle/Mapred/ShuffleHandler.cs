using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Util.Concurrent;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred.Proto;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Mapreduce.Task.Reduce;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security.Proto;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Fusesource.Leveldbjni;
using Org.Fusesource.Leveldbjni.Internal;
using Org.Iq80.Leveldb;
using Org.Jboss.Netty.Bootstrap;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Org.Jboss.Netty.Channel.Group;
using Org.Jboss.Netty.Channel.Socket.Nio;
using Org.Jboss.Netty.Handler.Codec.Frame;
using Org.Jboss.Netty.Handler.Codec.Http;
using Org.Jboss.Netty.Handler.Ssl;
using Org.Jboss.Netty.Handler.Stream;
using Org.Jboss.Netty.Util;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class ShuffleHandler : AuxiliaryService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.ShuffleHandler
			));

		public const string ShuffleManageOsCache = "mapreduce.shuffle.manage.os.cache";

		public const bool DefaultShuffleManageOsCache = true;

		public const string ShuffleReadaheadBytes = "mapreduce.shuffle.readahead.bytes";

		public const int DefaultShuffleReadaheadBytes = 4 * 1024 * 1024;

		private static readonly Sharpen.Pattern IgnorableErrorMessage = Sharpen.Pattern.Compile
			("^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$", Sharpen.Pattern.
			CaseInsensitive);

		private const string StateDbName = "mapreduce_shuffle_state";

		private const string StateDbSchemaVersionKey = "shuffle-schema-version";

		protected internal static readonly Version CurrentVersionInfo = Version.NewInstance
			(1, 0);

		private int port;

		private ChannelFactory selector;

		private readonly ChannelGroup accepted = new DefaultChannelGroup();

		protected internal ShuffleHandler.HttpPipelineFactory pipelineFact;

		private int sslFileBufferSize;

		/// <summary>
		/// Should the shuffle use posix_fadvise calls to manage the OS cache during
		/// sendfile
		/// </summary>
		private bool manageOsCache;

		private int readaheadLength;

		private int maxShuffleConnections;

		private int shuffleBufferSize;

		private bool shuffleTransferToAllowed;

		private int maxSessionOpenFiles;

		private ReadaheadPool readaheadPool = ReadaheadPool.GetInstance();

		private IDictionary<string, string> userRsrc;

		private JobTokenSecretManager secretManager;

		private DB stateDb = null;

		public const string MapreduceShuffleServiceid = "mapreduce_shuffle";

		public const string ShufflePortConfigKey = "mapreduce.shuffle.port";

		public const int DefaultShufflePort = 13562;

		public const string ShuffleConnectionKeepAliveEnabled = "mapreduce.shuffle.connection-keep-alive.enable";

		public const bool DefaultShuffleConnectionKeepAliveEnabled = false;

		public const string ShuffleConnectionKeepAliveTimeOut = "mapreduce.shuffle.connection-keep-alive.timeout";

		public const int DefaultShuffleConnectionKeepAliveTimeOut = 5;

		public const string ShuffleMapoutputMetaInfoCacheSize = "mapreduce.shuffle.mapoutput-info.meta.cache.size";

		public const int DefaultShuffleMapoutputMetaInfoCacheSize = 1000;

		public const string ConnectionClose = "close";

		public const string SuffleSslFileBufferSizeKey = "mapreduce.shuffle.ssl.file.buffer.size";

		public const int DefaultSuffleSslFileBufferSize = 60 * 1024;

		public const string MaxShuffleConnections = "mapreduce.shuffle.max.connections";

		public const int DefaultMaxShuffleConnections = 0;

		public const string MaxShuffleThreads = "mapreduce.shuffle.max.threads";

		public const int DefaultMaxShuffleThreads = 0;

		public const string ShuffleBufferSize = "mapreduce.shuffle.transfer.buffer.size";

		public const int DefaultShuffleBufferSize = 128 * 1024;

		public const string ShuffleTransfertoAllowed = "mapreduce.shuffle.transferTo.allowed";

		public const bool DefaultShuffleTransfertoAllowed = true;

		public const bool WindowsDefaultShuffleTransfertoAllowed = false;

		public const string ShuffleMaxSessionOpenFiles = "mapreduce.shuffle.max.session-open-files";

		public const int DefaultShuffleMaxSessionOpenFiles = 3;

		internal bool connectionKeepAliveEnabled = false;

		internal int connectionKeepAliveTimeOut;

		internal int mapOutputMetaInfoCacheSize;

		internal class ShuffleMetrics : ChannelFutureListener
		{
			internal MutableCounterLong shuffleOutputBytes;

			internal MutableCounterInt shuffleOutputsFailed;

			internal MutableCounterInt shuffleOutputsOK;

			internal MutableGaugeInt shuffleConnections;

			// pattern to identify errors related to the client closing the socket early
			// idea borrowed from Netty SslHandler
			//seconds
			// 0 implies no limit
			// 0 implies Netty default of 2 * number of available processors
			/* the maximum number of files a single GET request can
			open simultaneously during shuffle
			*/
			/// <exception cref="System.Exception"/>
			public override void OperationComplete(ChannelFuture future)
			{
				if (future.IsSuccess())
				{
					shuffleOutputsOK.Incr();
				}
				else
				{
					shuffleOutputsFailed.Incr();
				}
				shuffleConnections.Decr();
			}
		}

		internal readonly ShuffleHandler.ShuffleMetrics metrics;

		internal class ReduceMapFileCount : ChannelFutureListener
		{
			private ShuffleHandler.ReduceContext reduceContext;

			public ReduceMapFileCount(ShuffleHandler _enclosing, ShuffleHandler.ReduceContext
				 rc)
			{
				this._enclosing = _enclosing;
				this.reduceContext = rc;
			}

			/// <exception cref="System.Exception"/>
			public override void OperationComplete(ChannelFuture future)
			{
				if (!future.IsSuccess())
				{
					future.GetChannel().Close();
					return;
				}
				int waitCount = this.reduceContext.GetMapsToWait().DecrementAndGet();
				if (waitCount == 0)
				{
					this._enclosing.metrics.OperationComplete(future);
					future.GetChannel().Close();
				}
				else
				{
					this._enclosing.pipelineFact.GetSHUFFLE().SendMap(this.reduceContext);
				}
			}

			private readonly ShuffleHandler _enclosing;
		}

		/// <summary>Maintain parameters per messageReceived() Netty context.</summary>
		/// <remarks>
		/// Maintain parameters per messageReceived() Netty context.
		/// Allows sendMapOutput calls from operationComplete()
		/// </remarks>
		private class ReduceContext
		{
			private IList<string> mapIds;

			private AtomicInteger mapsToWait;

			private AtomicInteger mapsToSend;

			private int reduceId;

			private ChannelHandlerContext ctx;

			private string user;

			private IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> infoMap;

			private string outputBasePathStr;

			public ReduceContext(IList<string> mapIds, int rId, ChannelHandlerContext context
				, string usr, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> mapOutputInfoMap
				, string outputBasePath)
			{
				this.mapIds = mapIds;
				this.reduceId = rId;
				this.mapsToWait = new AtomicInteger(mapIds.Count);
				this.mapsToSend = new AtomicInteger(0);
				this.ctx = context;
				this.user = usr;
				this.infoMap = mapOutputInfoMap;
				this.outputBasePathStr = outputBasePath;
			}

			public virtual int GetReduceId()
			{
				return reduceId;
			}

			public virtual ChannelHandlerContext GetCtx()
			{
				return ctx;
			}

			public virtual string GetUser()
			{
				return user;
			}

			public virtual IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> GetInfoMap
				()
			{
				return infoMap;
			}

			public virtual string GetOutputBasePathStr()
			{
				return outputBasePathStr;
			}

			public virtual IList<string> GetMapIds()
			{
				return mapIds;
			}

			public virtual AtomicInteger GetMapsToSend()
			{
				return mapsToSend;
			}

			public virtual AtomicInteger GetMapsToWait()
			{
				return mapsToWait;
			}
		}

		internal ShuffleHandler(MetricsSystem ms)
			: base("httpshuffle")
		{
			metrics = ms.Register(new ShuffleHandler.ShuffleMetrics());
		}

		public ShuffleHandler()
			: this(DefaultMetricsSystem.Instance())
		{
		}

		/// <summary>Serialize the shuffle port into a ByteBuffer for use later on.</summary>
		/// <param name="port">the port to be sent to the ApplciationMaster</param>
		/// <returns>the serialized form of the port.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static ByteBuffer SerializeMetaData(int port)
		{
			//TODO these bytes should be versioned
			DataOutputBuffer port_dob = new DataOutputBuffer();
			port_dob.WriteInt(port);
			return ByteBuffer.Wrap(port_dob.GetData(), 0, port_dob.GetLength());
		}

		/// <summary>A helper function to deserialize the metadata returned by ShuffleHandler.
		/// 	</summary>
		/// <param name="meta">the metadata returned by the ShuffleHandler</param>
		/// <returns>the port the Shuffle Handler is listening on to serve shuffle data.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static int DeserializeMetaData(ByteBuffer meta)
		{
			//TODO this should be returning a class not just an int
			DataInputByteBuffer @in = new DataInputByteBuffer();
			@in.Reset(meta);
			int port = @in.ReadInt();
			return port;
		}

		/// <summary>
		/// A helper function to serialize the JobTokenIdentifier to be sent to the
		/// ShuffleHandler as ServiceData.
		/// </summary>
		/// <param name="jobToken">
		/// the job token to be used for authentication of
		/// shuffle data requests.
		/// </param>
		/// <returns>the serialized version of the jobToken.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static ByteBuffer SerializeServiceData(Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> jobToken)
		{
			//TODO these bytes should be versioned
			DataOutputBuffer jobToken_dob = new DataOutputBuffer();
			jobToken.Write(jobToken_dob);
			return ByteBuffer.Wrap(jobToken_dob.GetData(), 0, jobToken_dob.GetLength());
		}

		/// <exception cref="System.IO.IOException"/>
		internal static Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> DeserializeServiceData
			(ByteBuffer secret)
		{
			DataInputByteBuffer @in = new DataInputByteBuffer();
			@in.Reset(secret);
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = new Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier>();
			jt.ReadFields(@in);
			return jt;
		}

		public override void InitializeApplication(ApplicationInitializationContext context
			)
		{
			string user = context.GetUser();
			ApplicationId appId = context.GetApplicationId();
			ByteBuffer secret = context.GetApplicationDataForService();
			// TODO these bytes should be versioned
			try
			{
				Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jt = DeserializeServiceData
					(secret);
				// TODO: Once SHuffle is out of NM, this can use MR APIs
				JobID jobId = new JobID(System.Convert.ToString(appId.GetClusterTimestamp()), appId
					.GetId());
				RecordJobShuffleInfo(jobId, user, jt);
			}
			catch (IOException e)
			{
				Log.Error("Error during initApp", e);
			}
		}

		// TODO add API to AuxiliaryServices to report failures
		public override void StopApplication(ApplicationTerminationContext context)
		{
			ApplicationId appId = context.GetApplicationId();
			JobID jobId = new JobID(System.Convert.ToString(appId.GetClusterTimestamp()), appId
				.GetId());
			try
			{
				RemoveJobShuffleInfo(jobId);
			}
			catch (IOException e)
			{
				Log.Error("Error during stopApp", e);
			}
		}

		// TODO add API to AuxiliaryServices to report failures
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			manageOsCache = conf.GetBoolean(ShuffleManageOsCache, DefaultShuffleManageOsCache
				);
			readaheadLength = conf.GetInt(ShuffleReadaheadBytes, DefaultShuffleReadaheadBytes
				);
			maxShuffleConnections = conf.GetInt(MaxShuffleConnections, DefaultMaxShuffleConnections
				);
			int maxShuffleThreads = conf.GetInt(MaxShuffleThreads, DefaultMaxShuffleThreads);
			if (maxShuffleThreads == 0)
			{
				maxShuffleThreads = 2 * Runtime.GetRuntime().AvailableProcessors();
			}
			shuffleBufferSize = conf.GetInt(ShuffleBufferSize, DefaultShuffleBufferSize);
			shuffleTransferToAllowed = conf.GetBoolean(ShuffleTransfertoAllowed, (Shell.Windows
				) ? WindowsDefaultShuffleTransfertoAllowed : DefaultShuffleTransfertoAllowed);
			maxSessionOpenFiles = conf.GetInt(ShuffleMaxSessionOpenFiles, DefaultShuffleMaxSessionOpenFiles
				);
			ThreadFactory bossFactory = new ThreadFactoryBuilder().SetNameFormat("ShuffleHandler Netty Boss #%d"
				).Build();
			ThreadFactory workerFactory = new ThreadFactoryBuilder().SetNameFormat("ShuffleHandler Netty Worker #%d"
				).Build();
			selector = new NioServerSocketChannelFactory(Executors.NewCachedThreadPool(bossFactory
				), Executors.NewCachedThreadPool(workerFactory), maxShuffleThreads);
			base.ServiceInit(new Configuration(conf));
		}

		// TODO change AbstractService to throw InterruptedException
		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			userRsrc = new ConcurrentHashMap<string, string>();
			secretManager = new JobTokenSecretManager();
			RecoverState(conf);
			ServerBootstrap bootstrap = new ServerBootstrap(selector);
			try
			{
				pipelineFact = new ShuffleHandler.HttpPipelineFactory(this, conf);
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
			bootstrap.SetOption("child.keepAlive", true);
			bootstrap.SetPipelineFactory(pipelineFact);
			port = conf.GetInt(ShufflePortConfigKey, DefaultShufflePort);
			Org.Jboss.Netty.Channel.Channel ch = bootstrap.Bind(new IPEndPoint(port));
			accepted.AddItem(ch);
			port = ((IPEndPoint)ch.GetLocalAddress()).Port;
			conf.Set(ShufflePortConfigKey, Sharpen.Extensions.ToString(port));
			pipelineFact.Shuffle.SetPort(port);
			Log.Info(GetName() + " listening on port " + port);
			base.ServiceStart();
			sslFileBufferSize = conf.GetInt(SuffleSslFileBufferSizeKey, DefaultSuffleSslFileBufferSize
				);
			connectionKeepAliveEnabled = conf.GetBoolean(ShuffleConnectionKeepAliveEnabled, DefaultShuffleConnectionKeepAliveEnabled
				);
			connectionKeepAliveTimeOut = Math.Max(1, conf.GetInt(ShuffleConnectionKeepAliveTimeOut
				, DefaultShuffleConnectionKeepAliveTimeOut));
			mapOutputMetaInfoCacheSize = Math.Max(1, conf.GetInt(ShuffleMapoutputMetaInfoCacheSize
				, DefaultShuffleMapoutputMetaInfoCacheSize));
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			accepted.Close().AwaitUninterruptibly(10, TimeUnit.Seconds);
			if (selector != null)
			{
				ServerBootstrap bootstrap = new ServerBootstrap(selector);
				bootstrap.ReleaseExternalResources();
			}
			if (pipelineFact != null)
			{
				pipelineFact.Destroy();
			}
			if (stateDb != null)
			{
				stateDb.Close();
			}
			base.ServiceStop();
		}

		public override ByteBuffer GetMetaData()
		{
			lock (this)
			{
				try
				{
					return SerializeMetaData(port);
				}
				catch (IOException e)
				{
					Log.Error("Error during getMeta", e);
					// TODO add API to AuxiliaryServices to report failures
					return null;
				}
			}
		}

		protected internal virtual ShuffleHandler.Shuffle GetShuffle(Configuration conf)
		{
			return new ShuffleHandler.Shuffle(this, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RecoverState(Configuration conf)
		{
			Path recoveryRoot = GetRecoveryPath();
			if (recoveryRoot != null)
			{
				StartStore(recoveryRoot);
				Sharpen.Pattern jobPattern = Sharpen.Pattern.Compile(JobID.JobidRegex);
				LeveldbIterator iter = null;
				try
				{
					iter = new LeveldbIterator(stateDb);
					iter.Seek(JniDBFactory.Bytes(JobID.Job));
					while (iter.HasNext())
					{
						KeyValuePair<byte[], byte[]> entry = iter.Next();
						string key = JniDBFactory.AsString(entry.Key);
						if (!jobPattern.Matcher(key).Matches())
						{
							break;
						}
						RecoverJobShuffleInfo(key, entry.Value);
					}
				}
				catch (DBException e)
				{
					throw new IOException("Database error during recovery", e);
				}
				finally
				{
					if (iter != null)
					{
						iter.Close();
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartStore(Path recoveryRoot)
		{
			Options options = new Options();
			options.CreateIfMissing(false);
			options.Logger(new ShuffleHandler.LevelDBLogger());
			Path dbPath = new Path(recoveryRoot, StateDbName);
			Log.Info("Using state database at " + dbPath + " for recovery");
			FilePath dbfile = new FilePath(dbPath.ToString());
			try
			{
				stateDb = JniDBFactory.factory.Open(dbfile, options);
			}
			catch (NativeDB.DBException e)
			{
				if (e.IsNotFound() || e.Message.Contains(" does not exist "))
				{
					Log.Info("Creating state database at " + dbfile);
					options.CreateIfMissing(true);
					try
					{
						stateDb = JniDBFactory.factory.Open(dbfile, options);
						StoreVersion();
					}
					catch (DBException dbExc)
					{
						throw new IOException("Unable to create state store", dbExc);
					}
				}
				else
				{
					throw;
				}
			}
			CheckVersion();
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual Version LoadVersion()
		{
			byte[] data = stateDb.Get(JniDBFactory.Bytes(StateDbSchemaVersionKey));
			// if version is not stored previously, treat it as CURRENT_VERSION_INFO.
			if (data == null || data.Length == 0)
			{
				return GetCurrentVersion();
			}
			Version version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom
				(data));
			return version;
		}

		/// <exception cref="System.IO.IOException"/>
		private void StoreSchemaVersion(Version version)
		{
			string key = StateDbSchemaVersionKey;
			byte[] data = ((VersionPBImpl)version).GetProto().ToByteArray();
			try
			{
				stateDb.Put(JniDBFactory.Bytes(key), data);
			}
			catch (DBException e)
			{
				throw new IOException(e.Message, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void StoreVersion()
		{
			StoreSchemaVersion(CurrentVersionInfo);
		}

		// Only used for test
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void StoreVersion(Version version)
		{
			StoreSchemaVersion(version);
		}

		protected internal virtual Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <summary>1) Versioning scheme: major.minor.</summary>
		/// <remarks>
		/// 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
		/// 2) Any incompatible change of DB schema is a major upgrade, and any
		/// compatible change of DB schema is a minor upgrade.
		/// 3) Within a minor upgrade, say 1.1 to 1.2:
		/// overwrite the version info and proceed as normal.
		/// 4) Within a major upgrade, say 1.2 to 2.0:
		/// throw exception and indicate user to use a separate upgrade tool to
		/// upgrade shuffle info or remove incompatible old state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckVersion()
		{
			Version loadedVersion = LoadVersion();
			Log.Info("Loaded state DB schema version info " + loadedVersion);
			if (loadedVersion.Equals(GetCurrentVersion()))
			{
				return;
			}
			if (loadedVersion.IsCompatibleTo(GetCurrentVersion()))
			{
				Log.Info("Storing state DB schedma version info " + GetCurrentVersion());
				StoreVersion();
			}
			else
			{
				throw new IOException("Incompatible version for state DB schema: expecting DB schema version "
					 + GetCurrentVersion() + ", but loading version " + loadedVersion);
			}
		}

		private void AddJobToken(JobID jobId, string user, Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> jobToken)
		{
			userRsrc[jobId.ToString()] = user;
			secretManager.AddTokenForJob(jobId.ToString(), jobToken);
			Log.Info("Added token for " + jobId.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		private void RecoverJobShuffleInfo(string jobIdStr, byte[] data)
		{
			JobID jobId;
			try
			{
				jobId = ((JobID)JobID.ForName(jobIdStr));
			}
			catch (ArgumentException e)
			{
				throw new IOException("Bad job ID " + jobIdStr + " in state store", e);
			}
			ShuffleHandlerRecoveryProtos.JobShuffleInfoProto proto = ShuffleHandlerRecoveryProtos.JobShuffleInfoProto
				.ParseFrom(data);
			string user = proto.GetUser();
			SecurityProtos.TokenProto tokenProto = proto.GetJobToken();
			Org.Apache.Hadoop.Security.Token.Token<JobTokenIdentifier> jobToken = new Org.Apache.Hadoop.Security.Token.Token
				<JobTokenIdentifier>(tokenProto.GetIdentifier().ToByteArray(), tokenProto.GetPassword
				().ToByteArray(), new Text(tokenProto.GetKind()), new Text(tokenProto.GetService
				()));
			AddJobToken(jobId, user, jobToken);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RecordJobShuffleInfo(JobID jobId, string user, Org.Apache.Hadoop.Security.Token.Token
			<JobTokenIdentifier> jobToken)
		{
			if (stateDb != null)
			{
				SecurityProtos.TokenProto tokenProto = ((SecurityProtos.TokenProto)SecurityProtos.TokenProto
					.NewBuilder().SetIdentifier(ByteString.CopyFrom(jobToken.GetIdentifier())).SetPassword
					(ByteString.CopyFrom(jobToken.GetPassword())).SetKind(jobToken.GetKind().ToString
					()).SetService(jobToken.GetService().ToString()).Build());
				ShuffleHandlerRecoveryProtos.JobShuffleInfoProto proto = ((ShuffleHandlerRecoveryProtos.JobShuffleInfoProto
					)ShuffleHandlerRecoveryProtos.JobShuffleInfoProto.NewBuilder().SetUser(user).SetJobToken
					(tokenProto).Build());
				try
				{
					stateDb.Put(JniDBFactory.Bytes(jobId.ToString()), proto.ToByteArray());
				}
				catch (DBException e)
				{
					throw new IOException("Error storing " + jobId, e);
				}
			}
			AddJobToken(jobId, user, jobToken);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RemoveJobShuffleInfo(JobID jobId)
		{
			string jobIdStr = jobId.ToString();
			secretManager.RemoveTokenForJob(jobIdStr);
			Sharpen.Collections.Remove(userRsrc, jobIdStr);
			if (stateDb != null)
			{
				try
				{
					stateDb.Delete(JniDBFactory.Bytes(jobIdStr));
				}
				catch (DBException e)
				{
					throw new IOException("Unable to remove " + jobId + " from state store", e);
				}
			}
		}

		private class LevelDBLogger : Logger
		{
			private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
				ShuffleHandler.LevelDBLogger));

			public virtual void Log(string message)
			{
				Log.Info(message);
			}
		}

		internal class HttpPipelineFactory : ChannelPipelineFactory
		{
			internal readonly ShuffleHandler.Shuffle Shuffle;

			private SSLFactory sslFactory;

			/// <exception cref="System.Exception"/>
			public HttpPipelineFactory(ShuffleHandler _enclosing, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.Shuffle = this._enclosing.GetShuffle(conf);
				if (conf.GetBoolean(MRConfig.ShuffleSslEnabledKey, MRConfig.ShuffleSslEnabledDefault
					))
				{
					ShuffleHandler.Log.Info("Encrypted shuffle is enabled.");
					this.sslFactory = new SSLFactory(SSLFactory.Mode.Server, conf);
					this.sslFactory.Init();
				}
			}

			public virtual ShuffleHandler.Shuffle GetSHUFFLE()
			{
				return this.Shuffle;
			}

			public virtual void Destroy()
			{
				if (this.sslFactory != null)
				{
					this.sslFactory.Destroy();
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual ChannelPipeline GetPipeline()
			{
				ChannelPipeline pipeline = Channels.Pipeline();
				if (this.sslFactory != null)
				{
					pipeline.AddLast("ssl", new SslHandler(this.sslFactory.CreateSSLEngine()));
				}
				pipeline.AddLast("decoder", new HttpRequestDecoder());
				pipeline.AddLast("aggregator", new HttpChunkAggregator(1 << 16));
				pipeline.AddLast("encoder", new HttpResponseEncoder());
				pipeline.AddLast("chunking", new ChunkedWriteHandler());
				pipeline.AddLast("shuffle", this.Shuffle);
				return pipeline;
			}

			private readonly ShuffleHandler _enclosing;
			// TODO factor security manager into pipeline
			// TODO factor out encode/decode to permit binary shuffle
			// TODO factor out decode of index to permit alt. models
		}

		internal class Shuffle : SimpleChannelUpstreamHandler
		{
			private readonly Configuration conf;

			private readonly IndexCache indexCache;

			private readonly LocalDirAllocator lDirAlloc = new LocalDirAllocator(YarnConfiguration
				.NmLocalDirs);

			private int port;

			public Shuffle(ShuffleHandler _enclosing, Configuration conf)
			{
				this._enclosing = _enclosing;
				this.conf = conf;
				this.indexCache = new IndexCache(new JobConf(conf));
				this.port = conf.GetInt(ShuffleHandler.ShufflePortConfigKey, ShuffleHandler.DefaultShufflePort
					);
			}

			public virtual void SetPort(int port)
			{
				this.port = port;
			}

			private IList<string> SplitMaps(IList<string> mapq)
			{
				if (null == mapq)
				{
					return null;
				}
				IList<string> ret = new AList<string>();
				foreach (string s in mapq)
				{
					Sharpen.Collections.AddAll(ret, s.Split(","));
				}
				return ret;
			}

			/// <exception cref="System.Exception"/>
			public override void ChannelOpen(ChannelHandlerContext ctx, ChannelStateEvent evt
				)
			{
				if ((this._enclosing.maxShuffleConnections > 0) && (this._enclosing.accepted.Count
					 >= this._enclosing.maxShuffleConnections))
				{
					ShuffleHandler.Log.Info(string.Format("Current number of shuffle connections (%d) is "
						 + "greater than or equal to the max allowed shuffle connections (%d)", this._enclosing
						.accepted.Count, this._enclosing.maxShuffleConnections));
					evt.GetChannel().Close();
					return;
				}
				this._enclosing.accepted.AddItem(evt.GetChannel());
				base.ChannelOpen(ctx, evt);
			}

			/// <exception cref="System.Exception"/>
			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent evt)
			{
				HttpRequest request = (HttpRequest)evt.GetMessage();
				if (request.GetMethod() != HttpMethod.Get)
				{
					this.SendError(ctx, HttpResponseStatus.MethodNotAllowed);
					return;
				}
				// Check whether the shuffle version is compatible
				if (!ShuffleHeader.DefaultHttpHeaderName.Equals(request.GetHeader(ShuffleHeader.HttpHeaderName
					)) || !ShuffleHeader.DefaultHttpHeaderVersion.Equals(request.GetHeader(ShuffleHeader
					.HttpHeaderVersion)))
				{
					this.SendError(ctx, "Incompatible shuffle request version", HttpResponseStatus.BadRequest
						);
				}
				IDictionary<string, IList<string>> q = new QueryStringDecoder(request.GetUri()).GetParameters
					();
				IList<string> keepAliveList = q["keepAlive"];
				bool keepAliveParam = false;
				if (keepAliveList != null && keepAliveList.Count == 1)
				{
					keepAliveParam = Sharpen.Extensions.ValueOf(keepAliveList[0]);
					if (ShuffleHandler.Log.IsDebugEnabled())
					{
						ShuffleHandler.Log.Debug("KeepAliveParam : " + keepAliveList + " : " + keepAliveParam
							);
					}
				}
				IList<string> mapIds = this.SplitMaps(q["map"]);
				IList<string> reduceQ = q["reduce"];
				IList<string> jobQ = q["job"];
				if (ShuffleHandler.Log.IsDebugEnabled())
				{
					ShuffleHandler.Log.Debug("RECV: " + request.GetUri() + "\n  mapId: " + mapIds + "\n  reduceId: "
						 + reduceQ + "\n  jobId: " + jobQ + "\n  keepAlive: " + keepAliveParam);
				}
				if (mapIds == null || reduceQ == null || jobQ == null)
				{
					this.SendError(ctx, "Required param job, map and reduce", HttpResponseStatus.BadRequest
						);
					return;
				}
				if (reduceQ.Count != 1 || jobQ.Count != 1)
				{
					this.SendError(ctx, "Too many job/reduce parameters", HttpResponseStatus.BadRequest
						);
					return;
				}
				int reduceId;
				string jobId;
				try
				{
					reduceId = System.Convert.ToInt32(reduceQ[0]);
					jobId = jobQ[0];
				}
				catch (FormatException)
				{
					this.SendError(ctx, "Bad reduce parameter", HttpResponseStatus.BadRequest);
					return;
				}
				catch (ArgumentException)
				{
					this.SendError(ctx, "Bad job parameter", HttpResponseStatus.BadRequest);
					return;
				}
				string reqUri = request.GetUri();
				if (null == reqUri)
				{
					// TODO? add upstream?
					this.SendError(ctx, HttpResponseStatus.Forbidden);
					return;
				}
				HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus
					.Ok);
				try
				{
					this.VerifyRequest(jobId, ctx, request, response, new Uri("http", string.Empty, this
						.port, reqUri));
				}
				catch (IOException e)
				{
					ShuffleHandler.Log.Warn("Shuffle failure ", e);
					this.SendError(ctx, e.Message, HttpResponseStatus.Unauthorized);
					return;
				}
				IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> mapOutputInfoMap = new 
					Dictionary<string, ShuffleHandler.Shuffle.MapOutputInfo>();
				Org.Jboss.Netty.Channel.Channel ch = evt.GetChannel();
				string user = this._enclosing.userRsrc[jobId];
				// $x/$user/appcache/$appId/output/$mapId
				// TODO: Once Shuffle is out of NM, this can use MR APIs to convert
				// between App and Job
				string outputBasePathStr = this.GetBaseLocation(jobId, user);
				try
				{
					this.PopulateHeaders(mapIds, outputBasePathStr, user, reduceId, request, response
						, keepAliveParam, mapOutputInfoMap);
				}
				catch (IOException e)
				{
					ch.Write(response);
					ShuffleHandler.Log.Error("Shuffle error in populating headers :", e);
					string errorMessage = this.GetErrorMessage(e);
					this.SendError(ctx, errorMessage, HttpResponseStatus.InternalServerError);
					return;
				}
				ch.Write(response);
				//Initialize one ReduceContext object per messageReceived call
				ShuffleHandler.ReduceContext reduceContext = new ShuffleHandler.ReduceContext(mapIds
					, reduceId, ctx, user, mapOutputInfoMap, outputBasePathStr);
				for (int i = 0; i < Math.Min(this._enclosing.maxSessionOpenFiles, mapIds.Count); 
					i++)
				{
					ChannelFuture nextMap = this.SendMap(reduceContext);
					if (nextMap == null)
					{
						return;
					}
				}
			}

			/// <summary>
			/// Calls sendMapOutput for the mapId pointed by ReduceContext.mapsToSend
			/// and increments it.
			/// </summary>
			/// <remarks>
			/// Calls sendMapOutput for the mapId pointed by ReduceContext.mapsToSend
			/// and increments it. This method is first called by messageReceived()
			/// maxSessionOpenFiles times and then on the completion of every
			/// sendMapOutput operation. This limits the number of open files on a node,
			/// which can get really large(exhausting file descriptors on the NM) if all
			/// sendMapOutputs are called in one go, as was done previous to this change.
			/// </remarks>
			/// <param name="reduceContext">used to call sendMapOutput with correct params.</param>
			/// <returns>the ChannelFuture of the sendMapOutput, can be null.</returns>
			/// <exception cref="System.Exception"/>
			public virtual ChannelFuture SendMap(ShuffleHandler.ReduceContext reduceContext)
			{
				ChannelFuture nextMap = null;
				if (reduceContext.GetMapsToSend().Get() < reduceContext.GetMapIds().Count)
				{
					int nextIndex = reduceContext.GetMapsToSend().GetAndIncrement();
					string mapId = reduceContext.GetMapIds()[nextIndex];
					try
					{
						ShuffleHandler.Shuffle.MapOutputInfo info = reduceContext.GetInfoMap()[mapId];
						if (info == null)
						{
							info = this.GetMapOutputInfo(reduceContext.GetOutputBasePathStr() + mapId, mapId, 
								reduceContext.GetReduceId(), reduceContext.GetUser());
						}
						nextMap = this.SendMapOutput(reduceContext.GetCtx(), reduceContext.GetCtx().GetChannel
							(), reduceContext.GetUser(), mapId, reduceContext.GetReduceId(), info);
						if (null == nextMap)
						{
							this.SendError(reduceContext.GetCtx(), HttpResponseStatus.NotFound);
							return null;
						}
						nextMap.AddListener(new ShuffleHandler.ReduceMapFileCount(this, reduceContext));
					}
					catch (IOException e)
					{
						ShuffleHandler.Log.Error("Shuffle error :", e);
						string errorMessage = this.GetErrorMessage(e);
						this.SendError(reduceContext.GetCtx(), errorMessage, HttpResponseStatus.InternalServerError
							);
						return null;
					}
				}
				return nextMap;
			}

			private string GetErrorMessage(Exception t)
			{
				StringBuilder sb = new StringBuilder(t.Message);
				while (t.InnerException != null)
				{
					sb.Append(t.InnerException.Message);
					t = t.InnerException;
				}
				return sb.ToString();
			}

			private string GetBaseLocation(string jobId, string user)
			{
				JobID jobID = ((JobID)JobID.ForName(jobId));
				ApplicationId appID = ApplicationId.NewInstance(long.Parse(jobID.GetJtIdentifier(
					)), jobID.GetId());
				string baseStr = ContainerLocalizer.Usercache + "/" + user + "/" + ContainerLocalizer
					.Appcache + "/" + ConverterUtils.ToString(appID) + "/output" + "/";
				return baseStr;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual ShuffleHandler.Shuffle.MapOutputInfo GetMapOutputInfo(
				string @base, string mapId, int reduce, string user)
			{
				// Index file
				Path indexFileName = this.lDirAlloc.GetLocalPathToRead(@base + "/file.out.index", 
					this.conf);
				IndexRecord info = this.indexCache.GetIndexInformation(mapId, reduce, indexFileName
					, user);
				Path mapOutputFileName = this.lDirAlloc.GetLocalPathToRead(@base + "/file.out", this
					.conf);
				if (ShuffleHandler.Log.IsDebugEnabled())
				{
					ShuffleHandler.Log.Debug(@base + " : " + mapOutputFileName + " : " + indexFileName
						);
				}
				ShuffleHandler.Shuffle.MapOutputInfo outputInfo = new ShuffleHandler.Shuffle.MapOutputInfo
					(this, mapOutputFileName, info);
				return outputInfo;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void PopulateHeaders(IList<string> mapIds, string outputBaseStr
				, string user, int reduce, HttpRequest request, HttpResponse response, bool keepAliveParam
				, IDictionary<string, ShuffleHandler.Shuffle.MapOutputInfo> mapOutputInfoMap)
			{
				long contentLength = 0;
				foreach (string mapId in mapIds)
				{
					string @base = outputBaseStr + mapId;
					ShuffleHandler.Shuffle.MapOutputInfo outputInfo = this.GetMapOutputInfo(@base, mapId
						, reduce, user);
					if (mapOutputInfoMap.Count < this._enclosing.mapOutputMetaInfoCacheSize)
					{
						mapOutputInfoMap[mapId] = outputInfo;
					}
					// Index file
					Path indexFileName = this.lDirAlloc.GetLocalPathToRead(@base + "/file.out.index", 
						this.conf);
					IndexRecord info = this.indexCache.GetIndexInformation(mapId, reduce, indexFileName
						, user);
					ShuffleHeader header = new ShuffleHeader(mapId, info.partLength, info.rawLength, 
						reduce);
					DataOutputBuffer dob = new DataOutputBuffer();
					header.Write(dob);
					contentLength += info.partLength;
					contentLength += dob.GetLength();
				}
				// Now set the response headers.
				this.SetResponseHeaders(response, keepAliveParam, contentLength);
			}

			protected internal virtual void SetResponseHeaders(HttpResponse response, bool keepAliveParam
				, long contentLength)
			{
				if (!this._enclosing.connectionKeepAliveEnabled && !keepAliveParam)
				{
					ShuffleHandler.Log.Info("Setting connection close header...");
					response.SetHeader(HttpHeaders.Connection, ShuffleHandler.ConnectionClose);
				}
				else
				{
					response.SetHeader(HttpHeaders.ContentLength, contentLength.ToString());
					response.SetHeader(HttpHeaders.Connection, HttpHeaders.KeepAlive);
					response.SetHeader(HttpHeaders.KeepAlive, "timeout=" + this._enclosing.connectionKeepAliveTimeOut
						);
					ShuffleHandler.Log.Info("Content Length in shuffle : " + contentLength);
				}
			}

			internal class MapOutputInfo
			{
				internal readonly Path mapOutputFileName;

				internal readonly IndexRecord indexRecord;

				internal MapOutputInfo(Shuffle _enclosing, Path mapOutputFileName, IndexRecord indexRecord
					)
				{
					this._enclosing = _enclosing;
					this.mapOutputFileName = mapOutputFileName;
					this.indexRecord = indexRecord;
				}

				private readonly Shuffle _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual void VerifyRequest(string appid, ChannelHandlerContext
				 ctx, HttpRequest request, HttpResponse response, Uri requestUri)
			{
				SecretKey tokenSecret = this._enclosing.secretManager.RetrieveTokenSecret(appid);
				if (null == tokenSecret)
				{
					ShuffleHandler.Log.Info("Request for unknown token " + appid);
					throw new IOException("could not find jobid");
				}
				// string to encrypt
				string enc_str = SecureShuffleUtils.BuildMsgFrom(requestUri);
				// hash from the fetcher
				string urlHashStr = request.GetHeader(SecureShuffleUtils.HttpHeaderUrlHash);
				if (urlHashStr == null)
				{
					ShuffleHandler.Log.Info("Missing header hash for " + appid);
					throw new IOException("fetcher cannot be authenticated");
				}
				if (ShuffleHandler.Log.IsDebugEnabled())
				{
					int len = urlHashStr.Length;
					ShuffleHandler.Log.Debug("verifying request. enc_str=" + enc_str + "; hash=..." +
						 Sharpen.Runtime.Substring(urlHashStr, len - len / 2, len - 1));
				}
				// verify - throws exception
				SecureShuffleUtils.VerifyReply(urlHashStr, enc_str, tokenSecret);
				// verification passed - encode the reply
				string reply = SecureShuffleUtils.GenerateHash(Sharpen.Runtime.GetBytesForString(
					urlHashStr, Charsets.Utf8), tokenSecret);
				response.SetHeader(SecureShuffleUtils.HttpHeaderReplyUrlHash, reply);
				// Put shuffle version into http header
				response.SetHeader(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
					);
				response.SetHeader(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
					);
				if (ShuffleHandler.Log.IsDebugEnabled())
				{
					int len = reply.Length;
					ShuffleHandler.Log.Debug("Fetcher request verfied. enc_str=" + enc_str + ";reply="
						 + Sharpen.Runtime.Substring(reply, len - len / 2, len - 1));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual ChannelFuture SendMapOutput(ChannelHandlerContext ctx, 
				Org.Jboss.Netty.Channel.Channel ch, string user, string mapId, int reduce, ShuffleHandler.Shuffle.MapOutputInfo
				 mapOutputInfo)
			{
				IndexRecord info = mapOutputInfo.indexRecord;
				ShuffleHeader header = new ShuffleHeader(mapId, info.partLength, info.rawLength, 
					reduce);
				DataOutputBuffer dob = new DataOutputBuffer();
				header.Write(dob);
				ch.Write(ChannelBuffers.WrappedBuffer(dob.GetData(), 0, dob.GetLength()));
				FilePath spillfile = new FilePath(mapOutputInfo.mapOutputFileName.ToString());
				RandomAccessFile spill;
				try
				{
					spill = SecureIOUtils.OpenForRandomRead(spillfile, "r", user, null);
				}
				catch (FileNotFoundException)
				{
					ShuffleHandler.Log.Info(spillfile + " not found");
					return null;
				}
				ChannelFuture writeFuture;
				if (ch.GetPipeline().Get<SslHandler>() == null)
				{
					FadvisedFileRegion partition = new FadvisedFileRegion(spill, info.startOffset, info
						.partLength, this._enclosing.manageOsCache, this._enclosing.readaheadLength, this
						._enclosing.readaheadPool, spillfile.GetAbsolutePath(), this._enclosing.shuffleBufferSize
						, this._enclosing.shuffleTransferToAllowed);
					writeFuture = ch.Write(partition);
					writeFuture.AddListener(new _ChannelFutureListener_1135(partition));
				}
				else
				{
					// TODO error handling; distinguish IO/connection failures,
					//      attribute to appropriate spill output
					// HTTPS cannot be done with zero copy.
					FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill, info.startOffset, info
						.partLength, this._enclosing.sslFileBufferSize, this._enclosing.manageOsCache, this
						._enclosing.readaheadLength, this._enclosing.readaheadPool, spillfile.GetAbsolutePath
						());
					writeFuture = ch.Write(chunk);
				}
				this._enclosing.metrics.shuffleConnections.Incr();
				this._enclosing.metrics.shuffleOutputBytes.Incr(info.partLength);
				// optimistic
				return writeFuture;
			}

			private sealed class _ChannelFutureListener_1135 : ChannelFutureListener
			{
				public _ChannelFutureListener_1135(FadvisedFileRegion partition)
				{
					this.partition = partition;
				}

				public override void OperationComplete(ChannelFuture future)
				{
					if (future.IsSuccess())
					{
						partition.TransferSuccessful();
					}
					partition.ReleaseExternalResources();
				}

				private readonly FadvisedFileRegion partition;
			}

			protected internal virtual void SendError(ChannelHandlerContext ctx, HttpResponseStatus
				 status)
			{
				this.SendError(ctx, string.Empty, status);
			}

			protected internal virtual void SendError(ChannelHandlerContext ctx, string message
				, HttpResponseStatus status)
			{
				HttpResponse response = new DefaultHttpResponse(HttpVersion.Http11, status);
				response.SetHeader(HttpHeaders.Names.ContentType, "text/plain; charset=UTF-8");
				// Put shuffle version into http header
				response.SetHeader(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
					);
				response.SetHeader(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
					);
				response.SetContent(ChannelBuffers.CopiedBuffer(message, CharsetUtil.Utf8));
				// Close the connection as soon as the error message is sent.
				ctx.GetChannel().Write(response).AddListener(ChannelFutureListener.Close);
			}

			/// <exception cref="System.Exception"/>
			public override void ExceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			{
				Org.Jboss.Netty.Channel.Channel ch = e.GetChannel();
				Exception cause = e.GetCause();
				if (cause is TooLongFrameException)
				{
					this.SendError(ctx, HttpResponseStatus.BadRequest);
					return;
				}
				else
				{
					if (cause is IOException)
					{
						if (cause is ClosedChannelException)
						{
							ShuffleHandler.Log.Debug("Ignoring closed channel error", cause);
							return;
						}
						string message = cause.Message.ToString();
						if (ShuffleHandler.IgnorableErrorMessage.Matcher(message).Matches())
						{
							ShuffleHandler.Log.Debug("Ignoring client socket close", cause);
							return;
						}
					}
				}
				ShuffleHandler.Log.Error("Shuffle error: ", cause);
				if (ch.IsConnected())
				{
					ShuffleHandler.Log.Error("Shuffle error " + e);
					this.SendError(ctx, HttpResponseStatus.InternalServerError);
				}
			}

			private readonly ShuffleHandler _enclosing;
		}
	}
}
