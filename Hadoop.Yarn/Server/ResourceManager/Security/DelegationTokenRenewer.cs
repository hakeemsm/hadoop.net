using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security
{
	/// <summary>Service to renew application delegation tokens.</summary>
	public class DelegationTokenRenewer : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.DelegationTokenRenewer
			));

		public const string Scheme = "hdfs";

		private Timer renewalTimer;

		private RMContext rmContext;

		private DelegationTokenRenewer.DelegationTokenCancelThread dtCancelThread = new DelegationTokenRenewer.DelegationTokenCancelThread
			();

		private ThreadPoolExecutor renewerService;

		private ConcurrentMap<ApplicationId, ICollection<DelegationTokenRenewer.DelegationTokenToRenew
			>> appTokens = new ConcurrentHashMap<ApplicationId, ICollection<DelegationTokenRenewer.DelegationTokenToRenew
			>>();

		private ConcurrentMap<Org.Apache.Hadoop.Security.Token.Token<object>, DelegationTokenRenewer.DelegationTokenToRenew
			> allTokens = new ConcurrentHashMap<Org.Apache.Hadoop.Security.Token.Token<object
			>, DelegationTokenRenewer.DelegationTokenToRenew>();

		private readonly ConcurrentMap<ApplicationId, long> delayedRemovalMap = new ConcurrentHashMap
			<ApplicationId, long>();

		private long tokenRemovalDelayMs;

		private Sharpen.Thread delayedRemovalThread;

		private ReadWriteLock serviceStateLock = new ReentrantReadWriteLock();

		private volatile bool isServiceStarted;

		private LinkedBlockingQueue<DelegationTokenRenewer.DelegationTokenRenewerEvent> pendingEventQueue;

		private bool tokenKeepAliveEnabled;

		private bool hasProxyUserPrivileges;

		private long credentialsValidTimeRemaining;

		public const string RmSystemCredentialsValidTimeRemaining = YarnConfiguration.RmPrefix
			 + "system-credentials.valid-time-remaining";

		public const long DefaultRmSystemCredentialsValidTimeRemaining = 10800000;

		public DelegationTokenRenewer()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security.DelegationTokenRenewer
				).FullName)
		{
		}

		// global single timer (daemon)
		// delegation token canceler thread
		// this config is supposedly not used by end-users.
		// 3h
		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			this.hasProxyUserPrivileges = conf.GetBoolean(YarnConfiguration.RmProxyUserPrivilegesEnabled
				, YarnConfiguration.DefaultRmProxyUserPrivilegesEnabled);
			this.tokenKeepAliveEnabled = conf.GetBoolean(YarnConfiguration.LogAggregationEnabled
				, YarnConfiguration.DefaultLogAggregationEnabled);
			this.tokenRemovalDelayMs = conf.GetInt(YarnConfiguration.RmNmExpiryIntervalMs, YarnConfiguration
				.DefaultRmNmExpiryIntervalMs);
			this.credentialsValidTimeRemaining = conf.GetLong(RmSystemCredentialsValidTimeRemaining
				, DefaultRmSystemCredentialsValidTimeRemaining);
			SetLocalSecretManagerAndServiceAddr();
			renewerService = CreateNewThreadPoolService(conf);
			pendingEventQueue = new LinkedBlockingQueue<DelegationTokenRenewer.DelegationTokenRenewerEvent
				>();
			renewalTimer = new Timer(true);
			base.ServiceInit(conf);
		}

		protected internal virtual ThreadPoolExecutor CreateNewThreadPoolService(Configuration
			 conf)
		{
			int nThreads = conf.GetInt(YarnConfiguration.RmDelegationTokenRenewerThreadCount, 
				YarnConfiguration.DefaultRmDelegationTokenRenewerThreadCount);
			ThreadFactory tf = new ThreadFactoryBuilder().SetNameFormat("DelegationTokenRenewer #%d"
				).Build();
			ThreadPoolExecutor pool = new ThreadPoolExecutor(nThreads, nThreads, 3L, TimeUnit
				.Seconds, new LinkedBlockingQueue<Runnable>());
			pool.SetThreadFactory(tf);
			pool.AllowCoreThreadTimeOut(true);
			return pool;
		}

		// enable RM to short-circuit token operations directly to itself
		private void SetLocalSecretManagerAndServiceAddr()
		{
			RMDelegationTokenIdentifier.Renewer.SetSecretManager(rmContext.GetRMDelegationTokenSecretManager
				(), rmContext.GetClientRMService().GetBindAddress());
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			dtCancelThread.Start();
			if (tokenKeepAliveEnabled)
			{
				delayedRemovalThread = new Sharpen.Thread(new DelegationTokenRenewer.DelayedTokenRemovalRunnable
					(this, GetConfig()), "DelayedTokenCanceller");
				delayedRemovalThread.Start();
			}
			SetLocalSecretManagerAndServiceAddr();
			serviceStateLock.WriteLock().Lock();
			isServiceStarted = true;
			serviceStateLock.WriteLock().Unlock();
			while (!pendingEventQueue.IsEmpty())
			{
				ProcessDelegationTokenRenewerEvent(pendingEventQueue.Take());
			}
			base.ServiceStart();
		}

		private void ProcessDelegationTokenRenewerEvent(DelegationTokenRenewer.DelegationTokenRenewerEvent
			 evt)
		{
			serviceStateLock.ReadLock().Lock();
			try
			{
				if (isServiceStarted)
				{
					renewerService.Execute(new DelegationTokenRenewer.DelegationTokenRenewerRunnable(
						this, evt));
				}
				else
				{
					pendingEventQueue.AddItem(evt);
				}
			}
			finally
			{
				serviceStateLock.ReadLock().Unlock();
			}
		}

		protected override void ServiceStop()
		{
			if (renewalTimer != null)
			{
				renewalTimer.Cancel();
			}
			appTokens.Clear();
			allTokens.Clear();
			this.renewerService.Shutdown();
			dtCancelThread.Interrupt();
			try
			{
				dtCancelThread.Join(1000);
			}
			catch (Exception e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			if (tokenKeepAliveEnabled && delayedRemovalThread != null)
			{
				delayedRemovalThread.Interrupt();
				try
				{
					delayedRemovalThread.Join(1000);
				}
				catch (Exception e)
				{
					Log.Info("Interrupted while joining on delayed removal thread.", e);
				}
			}
		}

		/// <summary>class that is used for keeping tracks of DT to renew</summary>
		protected internal class DelegationTokenToRenew
		{
			public readonly Org.Apache.Hadoop.Security.Token.Token<object> token;

			public readonly ICollection<ApplicationId> referringAppIds;

			public readonly Configuration conf;

			public long expirationDate;

			public DelegationTokenRenewer.RenewalTimerTask timerTask;

			public volatile bool shouldCancelAtEnd;

			public long maxDate;

			public string user;

			public DelegationTokenToRenew(ICollection<ApplicationId> applicationIds, Org.Apache.Hadoop.Security.Token.Token
				<object> token, Configuration conf, long expirationDate, bool shouldCancelAtEnd, 
				string user)
			{
				this.token = token;
				this.user = user;
				if (token.GetKind().Equals(new Text("HDFS_DELEGATION_TOKEN")))
				{
					try
					{
						AbstractDelegationTokenIdentifier identifier = (AbstractDelegationTokenIdentifier
							)token.DecodeIdentifier();
						maxDate = identifier.GetMaxDate();
					}
					catch (IOException e)
					{
						throw new YarnRuntimeException(e);
					}
				}
				this.referringAppIds = Sharpen.Collections.SynchronizedSet(new HashSet<ApplicationId
					>(applicationIds));
				this.conf = conf;
				this.expirationDate = expirationDate;
				this.timerTask = null;
				this.shouldCancelAtEnd = shouldCancelAtEnd;
			}

			public virtual void SetTimerTask(DelegationTokenRenewer.RenewalTimerTask tTask)
			{
				timerTask = tTask;
			}

			[VisibleForTesting]
			public virtual void CancelTimer()
			{
				if (timerTask != null)
				{
					timerTask.Cancel();
				}
			}

			[VisibleForTesting]
			public virtual bool IsTimerCancelled()
			{
				return (timerTask != null) && timerTask.cancelled.Get();
			}

			public override string ToString()
			{
				return token + ";exp=" + expirationDate + "; apps=" + referringAppIds;
			}

			public override bool Equals(object obj)
			{
				return obj is DelegationTokenRenewer.DelegationTokenToRenew && token.Equals(((DelegationTokenRenewer.DelegationTokenToRenew
					)obj).token);
			}

			public override int GetHashCode()
			{
				return token.GetHashCode();
			}
		}

		private class DelegationTokenCancelThread : Sharpen.Thread
		{
			private class TokenWithConf
			{
				internal Org.Apache.Hadoop.Security.Token.Token<object> token;

				internal Configuration conf;

				internal TokenWithConf(Org.Apache.Hadoop.Security.Token.Token<object> token, Configuration
					 conf)
				{
					this.token = token;
					this.conf = conf;
				}
			}

			private LinkedBlockingQueue<DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf
				> queue = new LinkedBlockingQueue<DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf
				>();

			public DelegationTokenCancelThread()
				: base("Delegation Token Canceler")
			{
				SetDaemon(true);
			}

			public virtual void CancelToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> 
				token, Configuration conf)
				where _T0 : TokenIdentifier
			{
				DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf tokenWithConf = 
					new DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf(token, conf
					);
				while (!queue.Offer(tokenWithConf))
				{
					Log.Warn("Unable to add token " + token + " for cancellation. " + "Will retry..");
					try
					{
						Sharpen.Thread.Sleep(100);
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
			}

			public override void Run()
			{
				DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf tokenWithConf = 
					null;
				while (true)
				{
					try
					{
						tokenWithConf = queue.Take();
						DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf current = tokenWithConf;
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Cancelling token " + tokenWithConf.token.GetService());
						}
						// need to use doAs so that http can find the kerberos tgt
						UserGroupInformation.GetLoginUser().DoAs(new _PrivilegedExceptionAction_338(current
							));
					}
					catch (IOException e)
					{
						Log.Warn("Failed to cancel token " + tokenWithConf.token + " " + StringUtils.StringifyException
							(e));
					}
					catch (RuntimeException e)
					{
						Log.Warn("Failed to cancel token " + tokenWithConf.token + " " + StringUtils.StringifyException
							(e));
					}
					catch (Exception)
					{
						return;
					}
					catch (Exception t)
					{
						Log.Warn("Got exception " + StringUtils.StringifyException(t) + ". Exiting..");
						System.Environment.Exit(-1);
					}
				}
			}

			private sealed class _PrivilegedExceptionAction_338 : PrivilegedExceptionAction<Void
				>
			{
				public _PrivilegedExceptionAction_338(DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf
					 current)
				{
					this.current = current;
				}

				/// <exception cref="System.Exception"/>
				public Void Run()
				{
					current.token.Cancel(current.conf);
					return null;
				}

				private readonly DelegationTokenRenewer.DelegationTokenCancelThread.TokenWithConf
					 current;
			}
		}

		[VisibleForTesting]
		public virtual ICollection<Org.Apache.Hadoop.Security.Token.Token<object>> GetDelegationTokens
			()
		{
			ICollection<Org.Apache.Hadoop.Security.Token.Token<object>> tokens = new HashSet<
				Org.Apache.Hadoop.Security.Token.Token<object>>();
			foreach (ICollection<DelegationTokenRenewer.DelegationTokenToRenew> tokenList in 
				appTokens.Values)
			{
				foreach (DelegationTokenRenewer.DelegationTokenToRenew token in tokenList)
				{
					tokens.AddItem(token.token);
				}
			}
			return tokens;
		}

		/// <summary>Asynchronously add application tokens for renewal.</summary>
		/// <param name="applicationId">added application</param>
		/// <param name="ts">tokens</param>
		/// <param name="shouldCancelAtEnd">
		/// true if tokens should be canceled when the app is
		/// done else false.
		/// </param>
		/// <param name="user">user</param>
		public virtual void AddApplicationAsync(ApplicationId applicationId, Credentials 
			ts, bool shouldCancelAtEnd, string user)
		{
			ProcessDelegationTokenRenewerEvent(new DelegationTokenRenewer.DelegationTokenRenewerAppSubmitEvent
				(applicationId, ts, shouldCancelAtEnd, user));
		}

		/// <summary>Asynchronously add application tokens for renewal.</summary>
		/// <param name="applicationId">added application</param>
		/// <param name="ts">tokens</param>
		/// <param name="shouldCancelAtEnd">true if tokens should be canceled when the app is done else false.
		/// 	</param>
		/// <param name="user">user</param>
		public virtual void AddApplicationAsyncDuringRecovery(ApplicationId applicationId
			, Credentials ts, bool shouldCancelAtEnd, string user)
		{
			ProcessDelegationTokenRenewerEvent(new DelegationTokenRenewer.DelegationTokenRenewerAppRecoverEvent
				(applicationId, ts, shouldCancelAtEnd, user));
		}

		/// <summary>Synchronously renew delegation tokens.</summary>
		/// <param name="user">user</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void AddApplicationSync(ApplicationId applicationId, Credentials ts
			, bool shouldCancelAtEnd, string user)
		{
			HandleAppSubmitEvent(new DelegationTokenRenewer.DelegationTokenRenewerAppSubmitEvent
				(applicationId, ts, shouldCancelAtEnd, user));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void HandleAppSubmitEvent(DelegationTokenRenewer.AbstractDelegationTokenRenewerAppEvent
			 evt)
		{
			ApplicationId applicationId = evt.GetApplicationId();
			Credentials ts = evt.GetCredentials();
			bool shouldCancelAtEnd = evt.ShouldCancelAtEnd();
			if (ts == null)
			{
				return;
			}
			// nothing to add
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Registering tokens for renewal for:" + " appId = " + applicationId);
			}
			ICollection<Org.Apache.Hadoop.Security.Token.Token<object>> tokens = ts.GetAllTokens
				();
			long now = Runtime.CurrentTimeMillis();
			// find tokens for renewal, but don't add timers until we know
			// all renewable tokens are valid
			// At RM restart it is safe to assume that all the previously added tokens
			// are valid
			appTokens[applicationId] = Sharpen.Collections.SynchronizedSet(new HashSet<DelegationTokenRenewer.DelegationTokenToRenew
				>());
			ICollection<DelegationTokenRenewer.DelegationTokenToRenew> tokenList = new HashSet
				<DelegationTokenRenewer.DelegationTokenToRenew>();
			bool hasHdfsToken = false;
			foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in tokens)
			{
				if (token.IsManaged())
				{
					if (token.GetKind().Equals(new Text("HDFS_DELEGATION_TOKEN")))
					{
						Log.Info(applicationId + " found existing hdfs token " + token);
						hasHdfsToken = true;
					}
					DelegationTokenRenewer.DelegationTokenToRenew dttr = allTokens[token];
					if (dttr == null)
					{
						dttr = new DelegationTokenRenewer.DelegationTokenToRenew(Arrays.AsList(applicationId
							), token, GetConfig(), now, shouldCancelAtEnd, evt.GetUser());
						try
						{
							RenewToken(dttr);
						}
						catch (IOException ioe)
						{
							throw new IOException("Failed to renew token: " + dttr.token, ioe);
						}
					}
					tokenList.AddItem(dttr);
				}
			}
			if (!tokenList.IsEmpty())
			{
				// Renewing token and adding it to timer calls are separated purposefully
				// If user provides incorrect token then it should not be added for
				// renewal.
				foreach (DelegationTokenRenewer.DelegationTokenToRenew dtr in tokenList)
				{
					DelegationTokenRenewer.DelegationTokenToRenew currentDtr = allTokens.PutIfAbsent(
						dtr.token, dtr);
					if (currentDtr != null)
					{
						// another job beat us
						currentDtr.referringAppIds.AddItem(applicationId);
						appTokens[applicationId].AddItem(currentDtr);
					}
					else
					{
						appTokens[applicationId].AddItem(dtr);
						SetTimerForTokenRenewal(dtr);
					}
				}
			}
			if (!hasHdfsToken)
			{
				RequestNewHdfsDelegationToken(Arrays.AsList(applicationId), evt.GetUser(), shouldCancelAtEnd
					);
			}
		}

		/// <summary>Task - to renew a token</summary>
		private class RenewalTimerTask : TimerTask
		{
			private DelegationTokenRenewer.DelegationTokenToRenew dttr;

			private AtomicBoolean cancelled = new AtomicBoolean(false);

			internal RenewalTimerTask(DelegationTokenRenewer _enclosing, DelegationTokenRenewer.DelegationTokenToRenew
				 t)
			{
				this._enclosing = _enclosing;
				this.dttr = t;
			}

			public override void Run()
			{
				if (this.cancelled.Get())
				{
					return;
				}
				Org.Apache.Hadoop.Security.Token.Token<object> token = this.dttr.token;
				try
				{
					this._enclosing.RequestNewHdfsDelegationTokenIfNeeded(this.dttr);
					// if the token is not replaced by a new token, renew the token
					if (!this.dttr.IsTimerCancelled())
					{
						this._enclosing.RenewToken(this.dttr);
						this._enclosing.SetTimerForTokenRenewal(this.dttr);
					}
					else
					{
						// set the next one
						DelegationTokenRenewer.Log.Info("The token was removed already. Token = [" + this
							.dttr + "]");
					}
				}
				catch (Exception e)
				{
					DelegationTokenRenewer.Log.Error("Exception renewing token" + token + ". Not rescheduled"
						, e);
					this._enclosing.RemoveFailedDelegationToken(this.dttr);
				}
			}

			public override bool Cancel()
			{
				this.cancelled.Set(true);
				return base.Cancel();
			}

			private readonly DelegationTokenRenewer _enclosing;
		}

		/// <summary>set task to renew the token</summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual void SetTimerForTokenRenewal(DelegationTokenRenewer.DelegationTokenToRenew
			 token)
		{
			// calculate timer time
			long expiresIn = token.expirationDate - Runtime.CurrentTimeMillis();
			long renewIn = token.expirationDate - expiresIn / 10;
			// little bit before the expiration
			// need to create new task every time
			DelegationTokenRenewer.RenewalTimerTask tTask = new DelegationTokenRenewer.RenewalTimerTask
				(this, token);
			token.SetTimerTask(tTask);
			// keep reference to the timer
			renewalTimer.Schedule(token.timerTask, Sharpen.Extensions.CreateDate(renewIn));
			Log.Info("Renew " + token + " in " + expiresIn + " ms, appId = " + token.referringAppIds
				);
		}

		// renew a token
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		protected internal virtual void RenewToken(DelegationTokenRenewer.DelegationTokenToRenew
			 dttr)
		{
			// need to use doAs so that http can find the kerberos tgt
			// NOTE: token renewers should be responsible for the correct UGI!
			try
			{
				dttr.expirationDate = UserGroupInformation.GetLoginUser().DoAs(new _PrivilegedExceptionAction_558
					(dttr));
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			Log.Info("Renewed delegation-token= [" + dttr + "], for " + dttr.referringAppIds);
		}

		private sealed class _PrivilegedExceptionAction_558 : PrivilegedExceptionAction<long
			>
		{
			public _PrivilegedExceptionAction_558(DelegationTokenRenewer.DelegationTokenToRenew
				 dttr)
			{
				this.dttr = dttr;
			}

			/// <exception cref="System.Exception"/>
			public long Run()
			{
				return dttr.token.Renew(dttr.conf);
			}

			private readonly DelegationTokenRenewer.DelegationTokenToRenew dttr;
		}

		// Request new hdfs token if the token is about to expire, and remove the old
		// token from the tokenToRenew list
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void RequestNewHdfsDelegationTokenIfNeeded(DelegationTokenRenewer.DelegationTokenToRenew
			 dttr)
		{
			if (hasProxyUserPrivileges && dttr.maxDate - dttr.expirationDate < credentialsValidTimeRemaining
				 && dttr.token.GetKind().Equals(new Text("HDFS_DELEGATION_TOKEN")))
			{
				ICollection<ApplicationId> applicationIds;
				lock (dttr.referringAppIds)
				{
					applicationIds = new HashSet<ApplicationId>(dttr.referringAppIds);
					dttr.referringAppIds.Clear();
				}
				// remove all old expiring hdfs tokens for this application.
				foreach (ApplicationId appId in applicationIds)
				{
					ICollection<DelegationTokenRenewer.DelegationTokenToRenew> tokenSet = appTokens[appId
						];
					if (tokenSet == null || tokenSet.IsEmpty())
					{
						continue;
					}
					IEnumerator<DelegationTokenRenewer.DelegationTokenToRenew> iter = tokenSet.GetEnumerator
						();
					lock (tokenSet)
					{
						while (iter.HasNext())
						{
							DelegationTokenRenewer.DelegationTokenToRenew t = iter.Next();
							if (t.token.GetKind().Equals(new Text("HDFS_DELEGATION_TOKEN")))
							{
								iter.Remove();
								Sharpen.Collections.Remove(allTokens, t.token);
								t.CancelTimer();
								Log.Info("Removed expiring token " + t);
							}
						}
					}
				}
				Log.Info("Token= (" + dttr + ") is expiring, request new token.");
				RequestNewHdfsDelegationToken(applicationIds, dttr.user, dttr.shouldCancelAtEnd);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void RequestNewHdfsDelegationToken(ICollection<ApplicationId> referringAppIds
			, string user, bool shouldCancelAtEnd)
		{
			if (!hasProxyUserPrivileges)
			{
				Log.Info("RM proxy-user privilege is not enabled. Skip requesting hdfs tokens.");
				return;
			}
			// Get new hdfs tokens for this user
			Credentials credentials = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<object>[] newTokens = ObtainSystemTokensForUser
				(user, credentials);
			// Add new tokens to the toRenew list.
			Log.Info("Received new tokens for " + referringAppIds + ". Received " + newTokens
				.Length + " tokens.");
			if (newTokens.Length > 0)
			{
				foreach (Org.Apache.Hadoop.Security.Token.Token<object> token in newTokens)
				{
					if (token.IsManaged())
					{
						DelegationTokenRenewer.DelegationTokenToRenew tokenToRenew = new DelegationTokenRenewer.DelegationTokenToRenew
							(referringAppIds, token, GetConfig(), Time.Now(), shouldCancelAtEnd, user);
						// renew the token to get the next expiration date.
						RenewToken(tokenToRenew);
						SetTimerForTokenRenewal(tokenToRenew);
						foreach (ApplicationId applicationId in referringAppIds)
						{
							appTokens[applicationId].AddItem(tokenToRenew);
						}
						Log.Info("Received new token " + token);
					}
				}
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.WriteTokenStorageToStream(dob);
			ByteBuffer byteBuffer = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			foreach (ApplicationId applicationId_1 in referringAppIds)
			{
				rmContext.GetSystemCredentialsForApps()[applicationId_1] = byteBuffer;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		protected internal virtual Org.Apache.Hadoop.Security.Token.Token<object>[] ObtainSystemTokensForUser
			(string user, Credentials credentials)
		{
			// Get new hdfs tokens on behalf of this user
			UserGroupInformation proxyUser = UserGroupInformation.CreateProxyUser(user, UserGroupInformation
				.GetLoginUser());
			Org.Apache.Hadoop.Security.Token.Token<object>[] newTokens = proxyUser.DoAs(new _PrivilegedExceptionAction_658
				(this, credentials));
			// Close the FileSystem created by the new proxy user,
			// So that we don't leave an entry in the FileSystem cache
			return newTokens;
		}

		private sealed class _PrivilegedExceptionAction_658 : PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token
			<object>[]>
		{
			public _PrivilegedExceptionAction_658(DelegationTokenRenewer _enclosing, Credentials
				 credentials)
			{
				this._enclosing = _enclosing;
				this.credentials = credentials;
			}

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<object>[] Run()
			{
				FileSystem fs = FileSystem.Get(this._enclosing.GetConfig());
				try
				{
					return fs.AddDelegationTokens(UserGroupInformation.GetLoginUser().GetUserName(), 
						credentials);
				}
				finally
				{
					fs.Close();
				}
			}

			private readonly DelegationTokenRenewer _enclosing;

			private readonly Credentials credentials;
		}

		// cancel a token
		private void CancelToken(DelegationTokenRenewer.DelegationTokenToRenew t)
		{
			if (t.shouldCancelAtEnd)
			{
				dtCancelThread.CancelToken(t.token, t.conf);
			}
			else
			{
				Log.Info("Did not cancel " + t);
			}
		}

		/// <summary>removing failed DT</summary>
		private void RemoveFailedDelegationToken(DelegationTokenRenewer.DelegationTokenToRenew
			 t)
		{
			ICollection<ApplicationId> applicationIds = t.referringAppIds;
			lock (applicationIds)
			{
				Log.Error("removing failed delegation token for appid=" + applicationIds + ";t=" 
					+ t.token.GetService());
				foreach (ApplicationId applicationId in applicationIds)
				{
					appTokens[applicationId].Remove(t);
				}
			}
			Sharpen.Collections.Remove(allTokens, t.token);
			// cancel the timer
			t.CancelTimer();
		}

		/// <summary>Removing delegation token for completed applications.</summary>
		/// <param name="applicationId">completed application</param>
		public virtual void ApplicationFinished(ApplicationId applicationId)
		{
			ProcessDelegationTokenRenewerEvent(new DelegationTokenRenewer.DelegationTokenRenewerEvent
				(applicationId, DelegationTokenRenewer.DelegationTokenRenewerEventType.FinishApplication
				));
		}

		private void HandleAppFinishEvent(DelegationTokenRenewer.DelegationTokenRenewerEvent
			 evt)
		{
			if (!tokenKeepAliveEnabled)
			{
				RemoveApplicationFromRenewal(evt.GetApplicationId());
			}
			else
			{
				delayedRemovalMap[evt.GetApplicationId()] = Runtime.CurrentTimeMillis() + tokenRemovalDelayMs;
			}
		}

		/// <summary>Add a list of applications to the keep alive list.</summary>
		/// <remarks>
		/// Add a list of applications to the keep alive list. If an appId already
		/// exists, update it's keep-alive time.
		/// </remarks>
		/// <param name="appIds">the list of applicationIds to be kept alive.</param>
		public virtual void UpdateKeepAliveApplications(IList<ApplicationId> appIds)
		{
			if (tokenKeepAliveEnabled && appIds != null && appIds.Count > 0)
			{
				foreach (ApplicationId appId in appIds)
				{
					delayedRemovalMap[appId] = Runtime.CurrentTimeMillis() + tokenRemovalDelayMs;
				}
			}
		}

		private void RemoveApplicationFromRenewal(ApplicationId applicationId)
		{
			Sharpen.Collections.Remove(rmContext.GetSystemCredentialsForApps(), applicationId
				);
			ICollection<DelegationTokenRenewer.DelegationTokenToRenew> tokens = appTokens[applicationId
				];
			if (tokens != null && !tokens.IsEmpty())
			{
				lock (tokens)
				{
					IEnumerator<DelegationTokenRenewer.DelegationTokenToRenew> it = tokens.GetEnumerator
						();
					while (it.HasNext())
					{
						DelegationTokenRenewer.DelegationTokenToRenew dttr = it.Next();
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Removing delegation token for appId=" + applicationId + "; token=" + dttr
								.token.GetService());
						}
						// continue if the app list isn't empty
						lock (dttr.referringAppIds)
						{
							dttr.referringAppIds.Remove(applicationId);
							if (!dttr.referringAppIds.IsEmpty())
							{
								continue;
							}
						}
						// cancel the timer
						dttr.CancelTimer();
						// cancel the token
						CancelToken(dttr);
						it.Remove();
						Sharpen.Collections.Remove(allTokens, dttr.token);
					}
				}
			}
			if (tokens != null && tokens.IsEmpty())
			{
				Sharpen.Collections.Remove(appTokens, applicationId);
			}
		}

		/// <summary>
		/// Takes care of cancelling app delegation tokens after the configured
		/// cancellation delay, taking into consideration keep-alive requests.
		/// </summary>
		private class DelayedTokenRemovalRunnable : Runnable
		{
			private long waitTimeMs;

			internal DelayedTokenRemovalRunnable(DelegationTokenRenewer _enclosing, Configuration
				 conf)
			{
				this._enclosing = _enclosing;
				this.waitTimeMs = conf.GetLong(YarnConfiguration.RmDelayedDelegationTokenRemovalIntervalMs
					, YarnConfiguration.DefaultRmDelayedDelegationTokenRemovalIntervalMs);
			}

			public virtual void Run()
			{
				IList<ApplicationId> toCancel = new AList<ApplicationId>();
				while (!Sharpen.Thread.CurrentThread().IsInterrupted())
				{
					IEnumerator<KeyValuePair<ApplicationId, long>> it = this._enclosing.delayedRemovalMap
						.GetEnumerator();
					toCancel.Clear();
					while (it.HasNext())
					{
						KeyValuePair<ApplicationId, long> e = it.Next();
						if (e.Value < Runtime.CurrentTimeMillis())
						{
							toCancel.AddItem(e.Key);
						}
					}
					foreach (ApplicationId appId in toCancel)
					{
						this._enclosing.RemoveApplicationFromRenewal(appId);
						Sharpen.Collections.Remove(this._enclosing.delayedRemovalMap, appId);
					}
					lock (this)
					{
						try
						{
							Sharpen.Runtime.Wait(this, this.waitTimeMs);
						}
						catch (Exception)
						{
							DelegationTokenRenewer.Log.Info("Delayed Deletion Thread Interrupted. Shutting it down"
								);
							return;
						}
					}
				}
			}

			private readonly DelegationTokenRenewer _enclosing;
		}

		public virtual void SetRMContext(RMContext rmContext)
		{
			this.rmContext = rmContext;
		}

		private sealed class DelegationTokenRenewerRunnable : Runnable
		{
			private DelegationTokenRenewer.DelegationTokenRenewerEvent evt;

			public DelegationTokenRenewerRunnable(DelegationTokenRenewer _enclosing, DelegationTokenRenewer.DelegationTokenRenewerEvent
				 evt)
			{
				this._enclosing = _enclosing;
				/*
				* This will run as a separate thread and will process individual events. It
				* is done in this way to make sure that the token renewal as a part of
				* application submission and token removal as a part of application finish
				* is asynchronous in nature.
				*/
				this.evt = evt;
			}

			public void Run()
			{
				if (this.evt is DelegationTokenRenewer.DelegationTokenRenewerAppSubmitEvent)
				{
					DelegationTokenRenewer.DelegationTokenRenewerAppSubmitEvent appSubmitEvt = (DelegationTokenRenewer.DelegationTokenRenewerAppSubmitEvent
						)this.evt;
					this.HandleDTRenewerAppSubmitEvent(appSubmitEvt);
				}
				else
				{
					if (this.evt is DelegationTokenRenewer.DelegationTokenRenewerAppRecoverEvent)
					{
						DelegationTokenRenewer.DelegationTokenRenewerAppRecoverEvent appRecoverEvt = (DelegationTokenRenewer.DelegationTokenRenewerAppRecoverEvent
							)this.evt;
						this._enclosing.HandleDTRenewerAppRecoverEvent(appRecoverEvt);
					}
					else
					{
						if (this.evt.GetType().Equals(DelegationTokenRenewer.DelegationTokenRenewerEventType
							.FinishApplication))
						{
							this._enclosing.HandleAppFinishEvent(this.evt);
						}
					}
				}
			}

			private void HandleDTRenewerAppSubmitEvent(DelegationTokenRenewer.DelegationTokenRenewerAppSubmitEvent
				 @event)
			{
				/*
				* For applications submitted with delegation tokens we are not submitting
				* the application to scheduler from RMAppManager. Instead we are doing
				* it from here. The primary goal is to make token renewal as a part of
				* application submission asynchronous so that client thread is not
				* blocked during app submission.
				*/
				try
				{
					// Setup tokens for renewal
					this._enclosing.HandleAppSubmitEvent(@event);
					this._enclosing.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent
						(@event.GetApplicationId(), RMAppEventType.Start));
				}
				catch (Exception t)
				{
					DelegationTokenRenewer.Log.Warn("Unable to add the application to the delegation token renewer."
						, t);
					// Sending APP_REJECTED is fine, since we assume that the
					// RMApp is in NEW state and thus we havne't yet informed the
					// Scheduler about the existence of the application
					this._enclosing.rmContext.GetDispatcher().GetEventHandler().Handle(new RMAppEvent
						(@event.GetApplicationId(), RMAppEventType.AppRejected, t.Message));
				}
			}

			private readonly DelegationTokenRenewer _enclosing;
		}

		private void HandleDTRenewerAppRecoverEvent(DelegationTokenRenewer.DelegationTokenRenewerAppRecoverEvent
			 @event)
		{
			try
			{
				// Setup tokens for renewal during recovery
				this.HandleAppSubmitEvent(@event);
			}
			catch (Exception t)
			{
				Log.Warn("Unable to add the application to the delegation token renewer.", t);
			}
		}

		internal class DelegationTokenRenewerAppSubmitEvent : DelegationTokenRenewer.AbstractDelegationTokenRenewerAppEvent
		{
			public DelegationTokenRenewerAppSubmitEvent(ApplicationId appId, Credentials credentails
				, bool shouldCancelAtEnd, string user)
				: base(appId, credentails, shouldCancelAtEnd, user, DelegationTokenRenewer.DelegationTokenRenewerEventType
					.VerifyAndStartApplication)
			{
			}
		}

		internal class DelegationTokenRenewerAppRecoverEvent : DelegationTokenRenewer.AbstractDelegationTokenRenewerAppEvent
		{
			public DelegationTokenRenewerAppRecoverEvent(ApplicationId appId, Credentials credentails
				, bool shouldCancelAtEnd, string user)
				: base(appId, credentails, shouldCancelAtEnd, user, DelegationTokenRenewer.DelegationTokenRenewerEventType
					.RecoverApplication)
			{
			}
		}

		internal class AbstractDelegationTokenRenewerAppEvent : DelegationTokenRenewer.DelegationTokenRenewerEvent
		{
			private Credentials credentials;

			private bool shouldCancelAtEnd;

			private string user;

			public AbstractDelegationTokenRenewerAppEvent(ApplicationId appId, Credentials credentails
				, bool shouldCancelAtEnd, string user, DelegationTokenRenewer.DelegationTokenRenewerEventType
				 type)
				: base(appId, type)
			{
				this.credentials = credentails;
				this.shouldCancelAtEnd = shouldCancelAtEnd;
				this.user = user;
			}

			public virtual Credentials GetCredentials()
			{
				return credentials;
			}

			public virtual bool ShouldCancelAtEnd()
			{
				return shouldCancelAtEnd;
			}

			public virtual string GetUser()
			{
				return user;
			}
		}

		internal enum DelegationTokenRenewerEventType
		{
			VerifyAndStartApplication,
			RecoverApplication,
			FinishApplication
		}

		private class DelegationTokenRenewerEvent : AbstractEvent<DelegationTokenRenewer.DelegationTokenRenewerEventType
			>
		{
			private ApplicationId appId;

			public DelegationTokenRenewerEvent(ApplicationId appId, DelegationTokenRenewer.DelegationTokenRenewerEventType
				 type)
				: base(type)
			{
				this.appId = appId;
			}

			public virtual ApplicationId GetApplicationId()
			{
				return appId;
			}
		}

		// only for testing
		protected internal virtual ConcurrentMap<Org.Apache.Hadoop.Security.Token.Token<object
			>, DelegationTokenRenewer.DelegationTokenToRenew> GetAllTokens()
		{
			return allTokens;
		}
	}
}
