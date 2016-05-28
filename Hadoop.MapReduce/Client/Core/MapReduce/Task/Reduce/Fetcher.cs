using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	internal class Fetcher<K, V> : Sharpen.Thread
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.Fetcher
			));

		/// <summary>Number of ms before timing out a copy</summary>
		private const int DefaultStalledCopyTimeout = 3 * 60 * 1000;

		/// <summary>Basic/unit connection timeout (in milliseconds)</summary>
		private const int UnitConnectTimeout = 60 * 1000;

		private const int DefaultReadTimeout = 3 * 60 * 1000;

		protected internal readonly Reporter reporter;

		private enum ShuffleErrors
		{
			IoError,
			WrongLength,
			BadId,
			WrongMap,
			Connection,
			WrongReduce
		}

		private const string ShuffleErrGrpName = "Shuffle Errors";

		private readonly JobConf jobConf;

		private readonly Counters.Counter connectionErrs;

		private readonly Counters.Counter ioErrs;

		private readonly Counters.Counter wrongLengthErrs;

		private readonly Counters.Counter badIdErrs;

		private readonly Counters.Counter wrongMapErrs;

		private readonly Counters.Counter wrongReduceErrs;

		protected internal readonly MergeManager<K, V> merger;

		protected internal readonly ShuffleSchedulerImpl<K, V> scheduler;

		protected internal readonly ShuffleClientMetrics metrics;

		protected internal readonly ExceptionReporter exceptionReporter;

		protected internal readonly int id;

		private static int nextId = 0;

		protected internal readonly int reduce;

		private readonly int connectionTimeout;

		private readonly int readTimeout;

		private readonly int fetchRetryTimeout;

		private readonly int fetchRetryInterval;

		private readonly bool fetchRetryEnabled;

		private readonly SecretKey shuffleSecretKey;

		protected internal HttpURLConnection connection;

		private volatile bool stopped = false;

		private long retryStartTime = 0;

		private static bool sslShuffle;

		private static SSLFactory sslFactory;

		public Fetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, V> scheduler
			, MergeManager<K, V> merger, Reporter reporter, ShuffleClientMetrics metrics, ExceptionReporter
			 exceptionReporter, SecretKey shuffleKey)
			: this(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, shuffleKey
				, ++nextId)
		{
		}

		[VisibleForTesting]
		internal Fetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, V> 
			scheduler, MergeManager<K, V> merger, Reporter reporter, ShuffleClientMetrics metrics
			, ExceptionReporter exceptionReporter, SecretKey shuffleKey, int id)
		{
			/* Default read timeout (in milliseconds) */
			// Initiative value is 0, which means it hasn't retried yet.
			this.jobConf = job;
			this.reporter = reporter;
			this.scheduler = scheduler;
			this.merger = merger;
			this.metrics = metrics;
			this.exceptionReporter = exceptionReporter;
			this.id = id;
			this.reduce = reduceId.GetTaskID().GetId();
			this.shuffleSecretKey = shuffleKey;
			ioErrs = reporter.GetCounter(ShuffleErrGrpName, Fetcher.ShuffleErrors.IoError.ToString
				());
			wrongLengthErrs = reporter.GetCounter(ShuffleErrGrpName, Fetcher.ShuffleErrors.WrongLength
				.ToString());
			badIdErrs = reporter.GetCounter(ShuffleErrGrpName, Fetcher.ShuffleErrors.BadId.ToString
				());
			wrongMapErrs = reporter.GetCounter(ShuffleErrGrpName, Fetcher.ShuffleErrors.WrongMap
				.ToString());
			connectionErrs = reporter.GetCounter(ShuffleErrGrpName, Fetcher.ShuffleErrors.Connection
				.ToString());
			wrongReduceErrs = reporter.GetCounter(ShuffleErrGrpName, Fetcher.ShuffleErrors.WrongReduce
				.ToString());
			this.connectionTimeout = job.GetInt(MRJobConfig.ShuffleConnectTimeout, DefaultStalledCopyTimeout
				);
			this.readTimeout = job.GetInt(MRJobConfig.ShuffleReadTimeout, DefaultReadTimeout);
			this.fetchRetryInterval = job.GetInt(MRJobConfig.ShuffleFetchRetryIntervalMs, MRJobConfig
				.DefaultShuffleFetchRetryIntervalMs);
			this.fetchRetryTimeout = job.GetInt(MRJobConfig.ShuffleFetchRetryTimeoutMs, DefaultStalledCopyTimeout
				);
			bool shuffleFetchEnabledDefault = job.GetBoolean(YarnConfiguration.NmRecoveryEnabled
				, YarnConfiguration.DefaultNmRecoveryEnabled);
			this.fetchRetryEnabled = job.GetBoolean(MRJobConfig.ShuffleFetchRetryEnabled, shuffleFetchEnabledDefault
				);
			SetName("fetcher#" + id);
			SetDaemon(true);
			lock (typeof(Org.Apache.Hadoop.Mapreduce.Task.Reduce.Fetcher))
			{
				sslShuffle = job.GetBoolean(MRConfig.ShuffleSslEnabledKey, MRConfig.ShuffleSslEnabledDefault
					);
				if (sslShuffle && sslFactory == null)
				{
					sslFactory = new SSLFactory(SSLFactory.Mode.Client, job);
					try
					{
						sslFactory.Init();
					}
					catch (Exception ex)
					{
						sslFactory.Destroy();
						throw new RuntimeException(ex);
					}
				}
			}
		}

		public override void Run()
		{
			try
			{
				while (!stopped && !Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					MapHost host = null;
					try
					{
						// If merge is on, block
						merger.WaitForResource();
						// Get a host to shuffle from
						host = scheduler.GetHost();
						metrics.ThreadBusy();
						// Shuffle
						CopyFromHost(host);
					}
					finally
					{
						if (host != null)
						{
							scheduler.FreeHost(host);
							metrics.ThreadFree();
						}
					}
				}
			}
			catch (Exception)
			{
				return;
			}
			catch (Exception t)
			{
				exceptionReporter.ReportException(t);
			}
		}

		public override void Interrupt()
		{
			try
			{
				CloseConnection();
			}
			finally
			{
				base.Interrupt();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void ShutDown()
		{
			this.stopped = true;
			Interrupt();
			try
			{
				Join(5000);
			}
			catch (Exception ie)
			{
				Log.Warn("Got interrupt while joining " + GetName(), ie);
			}
			if (sslFactory != null)
			{
				sslFactory.Destroy();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual void OpenConnection(Uri url)
		{
			lock (this)
			{
				HttpURLConnection conn = (HttpURLConnection)url.OpenConnection();
				if (sslShuffle)
				{
					HttpsURLConnection httpsConn = (HttpsURLConnection)conn;
					try
					{
						httpsConn.SetSSLSocketFactory(sslFactory.CreateSSLSocketFactory());
					}
					catch (GeneralSecurityException ex)
					{
						throw new IOException(ex);
					}
					httpsConn.SetHostnameVerifier(sslFactory.GetHostnameVerifier());
				}
				connection = conn;
			}
		}

		protected internal virtual void CloseConnection()
		{
			lock (this)
			{
				// Note that HttpURLConnection::disconnect() doesn't trash the object.
				// connect() attempts to reconnect in a loop, possibly reversing this
				if (connection != null)
				{
					connection.Disconnect();
				}
			}
		}

		private void AbortConnect(MapHost host, ICollection<TaskAttemptID> remaining)
		{
			foreach (TaskAttemptID left in remaining)
			{
				scheduler.PutBackKnownMapOutput(host, left);
			}
			CloseConnection();
		}

		private DataInputStream OpenShuffleUrl(MapHost host, ICollection<TaskAttemptID> remaining
			, Uri url)
		{
			DataInputStream input = null;
			try
			{
				SetupConnectionsWithRetry(host, remaining, url);
				if (stopped)
				{
					AbortConnect(host, remaining);
				}
				else
				{
					input = new DataInputStream(connection.GetInputStream());
				}
			}
			catch (IOException ie)
			{
				bool connectExcpt = ie is ConnectException;
				ioErrs.Increment(1);
				Log.Warn("Failed to connect to " + host + " with " + remaining.Count + " map outputs"
					, ie);
				// If connect did not succeed, just mark all the maps as failed,
				// indirectly penalizing the host
				scheduler.HostFailed(host.GetHostName());
				foreach (TaskAttemptID left in remaining)
				{
					scheduler.CopyFailed(left, host, false, connectExcpt);
				}
				// Add back all the remaining maps, WITHOUT marking them as failed
				foreach (TaskAttemptID left_1 in remaining)
				{
					scheduler.PutBackKnownMapOutput(host, left_1);
				}
			}
			return input;
		}

		/// <summary>The crux of the matter...</summary>
		/// <param name="host">
		/// 
		/// <see cref="MapHost"/>
		/// from which we need to
		/// shuffle available map-outputs.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual void CopyFromHost(MapHost host)
		{
			// reset retryStartTime for a new host
			retryStartTime = 0;
			// Get completed maps on 'host'
			IList<TaskAttemptID> maps = scheduler.GetMapsForHost(host);
			// Sanity check to catch hosts with only 'OBSOLETE' maps, 
			// especially at the tail of large jobs
			if (maps.Count == 0)
			{
				return;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Fetcher " + id + " going to fetch from " + host + " for: " + maps);
			}
			// List of maps to be fetched yet
			ICollection<TaskAttemptID> remaining = new HashSet<TaskAttemptID>(maps);
			// Construct the url and connect
			Uri url = GetMapOutputURL(host, maps);
			DataInputStream input = OpenShuffleUrl(host, remaining, url);
			if (input == null)
			{
				return;
			}
			try
			{
				// Loop through available map-outputs and fetch them
				// On any error, faildTasks is not null and we exit
				// after putting back the remaining maps to the 
				// yet_to_be_fetched list and marking the failed tasks.
				TaskAttemptID[] failedTasks = null;
				while (!remaining.IsEmpty() && failedTasks == null)
				{
					try
					{
						failedTasks = CopyMapOutput(host, input, remaining, fetchRetryEnabled);
					}
					catch (IOException)
					{
						//
						// Setup connection again if disconnected by NM
						connection.Disconnect();
						// Get map output from remaining tasks only.
						url = GetMapOutputURL(host, remaining);
						input = OpenShuffleUrl(host, remaining, url);
						if (input == null)
						{
							return;
						}
					}
				}
				if (failedTasks != null && failedTasks.Length > 0)
				{
					Log.Warn("copyMapOutput failed for tasks " + Arrays.ToString(failedTasks));
					scheduler.HostFailed(host.GetHostName());
					foreach (TaskAttemptID left in failedTasks)
					{
						scheduler.CopyFailed(left, host, true, false);
					}
				}
				// Sanity check
				if (failedTasks == null && !remaining.IsEmpty())
				{
					throw new IOException("server didn't return all expected map outputs: " + remaining
						.Count + " left.");
				}
				input.Close();
				input = null;
			}
			finally
			{
				if (input != null)
				{
					IOUtils.Cleanup(Log, input);
					input = null;
				}
				foreach (TaskAttemptID left in remaining)
				{
					scheduler.PutBackKnownMapOutput(host, left);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetupConnectionsWithRetry(MapHost host, ICollection<TaskAttemptID> remaining
			, Uri url)
		{
			OpenConnectionWithRetry(host, remaining, url);
			if (stopped)
			{
				return;
			}
			// generate hash of the url
			string msgToEncode = SecureShuffleUtils.BuildMsgFrom(url);
			string encHash = SecureShuffleUtils.HashFromString(msgToEncode, shuffleSecretKey);
			SetupShuffleConnection(encHash);
			Connect(connection, connectionTimeout);
			// verify that the thread wasn't stopped during calls to connect
			if (stopped)
			{
				return;
			}
			VerifyConnection(url, msgToEncode, encHash);
		}

		/// <exception cref="System.IO.IOException"/>
		private void OpenConnectionWithRetry(MapHost host, ICollection<TaskAttemptID> remaining
			, Uri url)
		{
			long startTime = Time.MonotonicNow();
			bool shouldWait = true;
			while (shouldWait)
			{
				try
				{
					OpenConnection(url);
					shouldWait = false;
				}
				catch (IOException e)
				{
					if (!fetchRetryEnabled)
					{
						// throw exception directly if fetch's retry is not enabled
						throw;
					}
					if ((Time.MonotonicNow() - startTime) >= this.fetchRetryTimeout)
					{
						Log.Warn("Failed to connect to host: " + url + "after " + fetchRetryTimeout + " milliseconds."
							);
						throw;
					}
					try
					{
						Sharpen.Thread.Sleep(this.fetchRetryInterval);
					}
					catch (Exception)
					{
						if (stopped)
						{
							return;
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyConnection(Uri url, string msgToEncode, string encHash)
		{
			// Validate response code
			int rc = connection.GetResponseCode();
			if (rc != HttpURLConnection.HttpOk)
			{
				throw new IOException("Got invalid response code " + rc + " from " + url + ": " +
					 connection.GetResponseMessage());
			}
			// get the shuffle version
			if (!ShuffleHeader.DefaultHttpHeaderName.Equals(connection.GetHeaderField(ShuffleHeader
				.HttpHeaderName)) || !ShuffleHeader.DefaultHttpHeaderVersion.Equals(connection.GetHeaderField
				(ShuffleHeader.HttpHeaderVersion)))
			{
				throw new IOException("Incompatible shuffle response version");
			}
			// get the replyHash which is HMac of the encHash we sent to the server
			string replyHash = connection.GetHeaderField(SecureShuffleUtils.HttpHeaderReplyUrlHash
				);
			if (replyHash == null)
			{
				throw new IOException("security validation of TT Map output failed");
			}
			Log.Debug("url=" + msgToEncode + ";encHash=" + encHash + ";replyHash=" + replyHash
				);
			// verify that replyHash is HMac of encHash
			SecureShuffleUtils.VerifyReply(replyHash, encHash, shuffleSecretKey);
			Log.Info("for url=" + msgToEncode + " sent hash and received reply");
		}

		private void SetupShuffleConnection(string encHash)
		{
			// put url hash into http header
			connection.AddRequestProperty(SecureShuffleUtils.HttpHeaderUrlHash, encHash);
			// set the read timeout
			connection.SetReadTimeout(readTimeout);
			// put shuffle version into http header
			connection.AddRequestProperty(ShuffleHeader.HttpHeaderName, ShuffleHeader.DefaultHttpHeaderName
				);
			connection.AddRequestProperty(ShuffleHeader.HttpHeaderVersion, ShuffleHeader.DefaultHttpHeaderVersion
				);
		}

		private static TaskAttemptID[] EmptyAttemptIdArray = new TaskAttemptID[0];

		/// <exception cref="System.IO.IOException"/>
		private TaskAttemptID[] CopyMapOutput(MapHost host, DataInputStream input, ICollection
			<TaskAttemptID> remaining, bool canRetry)
		{
			MapOutput<K, V> mapOutput = null;
			TaskAttemptID mapId = null;
			long decompressedLength = -1;
			long compressedLength = -1;
			try
			{
				long startTime = Time.MonotonicNow();
				int forReduce = -1;
				//Read the shuffle header
				try
				{
					ShuffleHeader header = new ShuffleHeader();
					header.ReadFields(input);
					mapId = TaskAttemptID.ForName(header.mapId);
					compressedLength = header.compressedLength;
					decompressedLength = header.uncompressedLength;
					forReduce = header.forReduce;
				}
				catch (ArgumentException e)
				{
					badIdErrs.Increment(1);
					Log.Warn("Invalid map id ", e);
					//Don't know which one was bad, so consider all of them as bad
					return Sharpen.Collections.ToArray(remaining, new TaskAttemptID[remaining.Count]);
				}
				InputStream @is = input;
				@is = CryptoUtils.WrapIfNecessary(jobConf, @is, compressedLength);
				compressedLength -= CryptoUtils.CryptoPadding(jobConf);
				decompressedLength -= CryptoUtils.CryptoPadding(jobConf);
				// Do some basic sanity verification
				if (!VerifySanity(compressedLength, decompressedLength, forReduce, remaining, mapId
					))
				{
					return new TaskAttemptID[] { mapId };
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("header: " + mapId + ", len: " + compressedLength + ", decomp len: " + 
						decompressedLength);
				}
				// Get the location for the map output - either in-memory or on-disk
				try
				{
					mapOutput = merger.Reserve(mapId, decompressedLength, id);
				}
				catch (IOException ioe)
				{
					// kill this reduce attempt
					ioErrs.Increment(1);
					scheduler.ReportLocalError(ioe);
					return EmptyAttemptIdArray;
				}
				// Check if we can shuffle *now* ...
				if (mapOutput == null)
				{
					Log.Info("fetcher#" + id + " - MergeManager returned status WAIT ...");
					//Not an error but wait to process data.
					return EmptyAttemptIdArray;
				}
				// The codec for lz0,lz4,snappy,bz2,etc. throw java.lang.InternalError
				// on decompression failures. Catching and re-throwing as IOException
				// to allow fetch failure logic to be processed
				try
				{
					// Go!
					Log.Info("fetcher#" + id + " about to shuffle output of map " + mapOutput.GetMapId
						() + " decomp: " + decompressedLength + " len: " + compressedLength + " to " + mapOutput
						.GetDescription());
					mapOutput.Shuffle(host, @is, compressedLength, decompressedLength, metrics, reporter
						);
				}
				catch (InternalError e)
				{
					Log.Warn("Failed to shuffle for fetcher#" + id, e);
					throw new IOException(e);
				}
				// Inform the shuffle scheduler
				long endTime = Time.MonotonicNow();
				// Reset retryStartTime as map task make progress if retried before.
				retryStartTime = 0;
				scheduler.CopySucceeded(mapId, host, compressedLength, startTime, endTime, mapOutput
					);
				// Note successful shuffle
				remaining.Remove(mapId);
				metrics.SuccessFetch();
				return null;
			}
			catch (IOException ioe)
			{
				if (mapOutput != null)
				{
					mapOutput.Abort();
				}
				if (canRetry)
				{
					CheckTimeoutOrRetry(host, ioe);
				}
				ioErrs.Increment(1);
				if (mapId == null || mapOutput == null)
				{
					Log.Warn("fetcher#" + id + " failed to read map header" + mapId + " decomp: " + decompressedLength
						 + ", " + compressedLength, ioe);
					if (mapId == null)
					{
						return Sharpen.Collections.ToArray(remaining, new TaskAttemptID[remaining.Count]);
					}
					else
					{
						return new TaskAttemptID[] { mapId };
					}
				}
				Log.Warn("Failed to shuffle output of " + mapId + " from " + host.GetHostName(), 
					ioe);
				// Inform the shuffle-scheduler
				metrics.FailedFetch();
				return new TaskAttemptID[] { mapId };
			}
		}

		/// <summary>
		/// check if hit timeout of retry, if not, throw an exception and start a
		/// new round of retry.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void CheckTimeoutOrRetry(MapHost host, IOException ioe)
		{
			// First time to retry.
			long currentTime = Time.MonotonicNow();
			if (retryStartTime == 0)
			{
				retryStartTime = currentTime;
			}
			// Retry is not timeout, let's do retry with throwing an exception.
			if (currentTime - retryStartTime < this.fetchRetryTimeout)
			{
				Log.Warn("Shuffle output from " + host.GetHostName() + " failed, retry it.", ioe);
				throw ioe;
			}
			else
			{
				// timeout, prepare to be failed.
				Log.Warn("Timeout for copying MapOutput with retry on host " + host + "after " + 
					fetchRetryTimeout + " milliseconds.");
			}
		}

		/// <summary>Do some basic verification on the input received -- Being defensive</summary>
		/// <param name="compressedLength"/>
		/// <param name="decompressedLength"/>
		/// <param name="forReduce"/>
		/// <param name="remaining"/>
		/// <param name="mapId"/>
		/// <returns>true/false, based on if the verification succeeded or not</returns>
		private bool VerifySanity(long compressedLength, long decompressedLength, int forReduce
			, ICollection<TaskAttemptID> remaining, TaskAttemptID mapId)
		{
			if (compressedLength < 0 || decompressedLength < 0)
			{
				wrongLengthErrs.Increment(1);
				Log.Warn(GetName() + " invalid lengths in map output header: id: " + mapId + " len: "
					 + compressedLength + ", decomp len: " + decompressedLength);
				return false;
			}
			if (forReduce != reduce)
			{
				wrongReduceErrs.Increment(1);
				Log.Warn(GetName() + " data for the wrong reduce map: " + mapId + " len: " + compressedLength
					 + " decomp len: " + decompressedLength + " for reduce " + forReduce);
				return false;
			}
			// Sanity check
			if (!remaining.Contains(mapId))
			{
				wrongMapErrs.Increment(1);
				Log.Warn("Invalid map-output! Received output for " + mapId);
				return false;
			}
			return true;
		}

		/// <summary>Create the map-output-url.</summary>
		/// <remarks>
		/// Create the map-output-url. This will contain all the map ids
		/// separated by commas
		/// </remarks>
		/// <param name="host"/>
		/// <param name="maps"/>
		/// <returns/>
		/// <exception cref="System.UriFormatException"/>
		private Uri GetMapOutputURL(MapHost host, ICollection<TaskAttemptID> maps)
		{
			// Get the base url
			StringBuilder url = new StringBuilder(host.GetBaseUrl());
			bool first = true;
			foreach (TaskAttemptID mapId in maps)
			{
				if (!first)
				{
					url.Append(",");
				}
				url.Append(mapId);
				first = false;
			}
			Log.Debug("MapOutput URL for " + host + " -> " + url.ToString());
			return new Uri(url.ToString());
		}

		/// <summary>
		/// The connection establishment is attempted multiple times and is given up
		/// only on the last failure.
		/// </summary>
		/// <remarks>
		/// The connection establishment is attempted multiple times and is given up
		/// only on the last failure. Instead of connecting with a timeout of
		/// X, we try connecting with a timeout of x &lt; X but multiple times.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Connect(URLConnection connection, int connectionTimeout)
		{
			int unit = 0;
			if (connectionTimeout < 0)
			{
				throw new IOException("Invalid timeout " + "[timeout = " + connectionTimeout + " ms]"
					);
			}
			else
			{
				if (connectionTimeout > 0)
				{
					unit = Math.Min(UnitConnectTimeout, connectionTimeout);
				}
			}
			long startTime = Time.MonotonicNow();
			long lastTime = startTime;
			int attempts = 0;
			// set the connect timeout to the unit-connect-timeout
			connection.SetConnectTimeout(unit);
			while (true)
			{
				try
				{
					attempts++;
					connection.Connect();
					break;
				}
				catch (IOException ioe)
				{
					long currentTime = Time.MonotonicNow();
					long retryTime = currentTime - startTime;
					long leftTime = connectionTimeout - retryTime;
					long timeSinceLastIteration = currentTime - lastTime;
					// throw an exception if we have waited for timeout amount of time
					// note that the updated value if timeout is used here
					if (leftTime <= 0)
					{
						int retryTimeInSeconds = (int)retryTime / 1000;
						Log.Error("Connection retry failed with " + attempts + " attempts in " + retryTimeInSeconds
							 + " seconds");
						throw;
					}
					// reset the connect timeout for the last try
					if (leftTime < unit)
					{
						unit = (int)leftTime;
						// reset the connect time out for the final connect
						connection.SetConnectTimeout(unit);
					}
					if (timeSinceLastIteration < unit)
					{
						try
						{
							// sleep the left time of unit
							Sleep(unit - timeSinceLastIteration);
						}
						catch (Exception)
						{
							Log.Warn("Sleep in connection retry get interrupted.");
							if (stopped)
							{
								return;
							}
						}
					}
					// update the total remaining connect-timeout
					lastTime = Time.MonotonicNow();
				}
			}
		}
	}
}
