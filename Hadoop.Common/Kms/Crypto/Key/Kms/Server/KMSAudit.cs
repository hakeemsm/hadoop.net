using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Security;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// Provides convenience methods for audit logging consistently the different
	/// types of events.
	/// </summary>
	public class KMSAudit
	{
		private class AuditEvent
		{
			private readonly AtomicLong accessCount = new AtomicLong(-1);

			private readonly string keyName;

			private readonly string user;

			private readonly KMS.KMSOp op;

			private readonly string extraMsg;

			private readonly long startTime = Runtime.CurrentTimeMillis();

			private AuditEvent(string keyName, string user, KMS.KMSOp op, string msg)
			{
				this.keyName = keyName;
				this.user = user;
				this.op = op;
				this.extraMsg = msg;
			}

			public virtual string GetExtraMsg()
			{
				return extraMsg;
			}

			public virtual AtomicLong GetAccessCount()
			{
				return accessCount;
			}

			public virtual string GetKeyName()
			{
				return keyName;
			}

			public virtual string GetUser()
			{
				return user;
			}

			public virtual KMS.KMSOp GetOp()
			{
				return op;
			}

			public virtual long GetStartTime()
			{
				return startTime;
			}
		}

		public enum OpStatus
		{
			Ok,
			Unauthorized,
			Unauthenticated,
			Error
		}

		private static ICollection<KMS.KMSOp> AggregateOpsWhitelist = Sets.NewHashSet(KMS.KMSOp
			.GetKeyVersion, KMS.KMSOp.GetCurrentKey, KMS.KMSOp.DecryptEek, KMS.KMSOp.GenerateEek
			);

		private Com.Google.Common.Cache.Cache<string, KMSAudit.AuditEvent> cache;

		private ScheduledExecutorService executor;

		public const string KmsLoggerName = "kms-audit";

		private static Logger AuditLog = LoggerFactory.GetLogger(KmsLoggerName);

		/// <summary>Create a new KMSAudit.</summary>
		/// <param name="windowMs">
		/// Duplicate events within the aggregation window are quashed
		/// to reduce log traffic. A single message for aggregated
		/// events is printed at the end of the window, along with a
		/// count of the number of aggregated events.
		/// </param>
		internal KMSAudit(long windowMs)
		{
			cache = CacheBuilder.NewBuilder().ExpireAfterWrite(windowMs, TimeUnit.Milliseconds
				).RemovalListener(new _RemovalListener_118(this)).Build();
			executor = Executors.NewScheduledThreadPool(1, new ThreadFactoryBuilder().SetDaemon
				(true).SetNameFormat(KmsLoggerName + "_thread").Build());
			executor.ScheduleAtFixedRate(new _Runnable_132(this), windowMs / 10, windowMs / 10
				, TimeUnit.Milliseconds);
		}

		private sealed class _RemovalListener_118 : RemovalListener<string, KMSAudit.AuditEvent
			>
		{
			public _RemovalListener_118(KMSAudit _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void OnRemoval(RemovalNotification<string, KMSAudit.AuditEvent> entry)
			{
				KMSAudit.AuditEvent @event = entry.Value;
				if (@event.GetAccessCount().Get() > 0)
				{
					this._enclosing.LogEvent(@event);
					@event.GetAccessCount().Set(0);
					this._enclosing.cache.Put(entry.Key, @event);
				}
			}

			private readonly KMSAudit _enclosing;
		}

		private sealed class _Runnable_132 : Runnable
		{
			public _Runnable_132(KMSAudit _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				this._enclosing.cache.CleanUp();
			}

			private readonly KMSAudit _enclosing;
		}

		private void LogEvent(KMSAudit.AuditEvent @event)
		{
			AuditLog.Info("OK[op={}, key={}, user={}, accessCount={}, interval={}ms] {}", @event
				.GetOp(), @event.GetKeyName(), @event.GetUser(), @event.GetAccessCount().Get(), 
				(Runtime.CurrentTimeMillis() - @event.GetStartTime()), @event.GetExtraMsg());
		}

		private void Op(KMSAudit.OpStatus opStatus, KMS.KMSOp op, string user, string key
			, string extraMsg)
		{
			if (!Strings.IsNullOrEmpty(user) && !Strings.IsNullOrEmpty(key) && (op != null) &&
				 AggregateOpsWhitelist.Contains(op))
			{
				string cacheKey = CreateCacheKey(user, key, op);
				if (opStatus == KMSAudit.OpStatus.Unauthorized)
				{
					cache.Invalidate(cacheKey);
					AuditLog.Info("UNAUTHORIZED[op={}, key={}, user={}] {}", op, key, user, extraMsg);
				}
				else
				{
					try
					{
						KMSAudit.AuditEvent @event = cache.Get(cacheKey, new _Callable_161(key, user, op, 
							extraMsg));
						// Log first access (initialized as -1 so
						// incrementAndGet() == 0 implies first access)
						if (@event.GetAccessCount().IncrementAndGet() == 0)
						{
							@event.GetAccessCount().IncrementAndGet();
							LogEvent(@event);
						}
					}
					catch (ExecutionException ex)
					{
						throw new RuntimeException(ex);
					}
				}
			}
			else
			{
				IList<string> kvs = new List<string>();
				if (op != null)
				{
					kvs.AddItem("op=" + op);
				}
				if (!Strings.IsNullOrEmpty(key))
				{
					kvs.AddItem("key=" + key);
				}
				if (!Strings.IsNullOrEmpty(user))
				{
					kvs.AddItem("user=" + user);
				}
				if (kvs.Count == 0)
				{
					AuditLog.Info("{} {}", opStatus.ToString(), extraMsg);
				}
				else
				{
					string join = Joiner.On(", ").Join(kvs);
					AuditLog.Info("{}[{}] {}", opStatus.ToString(), join, extraMsg);
				}
			}
		}

		private sealed class _Callable_161 : Callable<KMSAudit.AuditEvent>
		{
			public _Callable_161(string key, string user, KMS.KMSOp op, string extraMsg)
			{
				this.key = key;
				this.user = user;
				this.op = op;
				this.extraMsg = extraMsg;
			}

			/// <exception cref="System.Exception"/>
			public KMSAudit.AuditEvent Call()
			{
				return new KMSAudit.AuditEvent(key, user, op, extraMsg);
			}

			private readonly string key;

			private readonly string user;

			private readonly KMS.KMSOp op;

			private readonly string extraMsg;
		}

		public virtual void Ok(UserGroupInformation user, KMS.KMSOp op, string key, string
			 extraMsg)
		{
			Op(KMSAudit.OpStatus.Ok, op, user.GetShortUserName(), key, extraMsg);
		}

		public virtual void Ok(UserGroupInformation user, KMS.KMSOp op, string extraMsg)
		{
			Op(KMSAudit.OpStatus.Ok, op, user.GetShortUserName(), null, extraMsg);
		}

		public virtual void Unauthorized(UserGroupInformation user, KMS.KMSOp op, string 
			key)
		{
			Op(KMSAudit.OpStatus.Unauthorized, op, user.GetShortUserName(), key, string.Empty
				);
		}

		public virtual void Error(UserGroupInformation user, string method, string url, string
			 extraMsg)
		{
			Op(KMSAudit.OpStatus.Error, null, user.GetShortUserName(), null, "Method:'" + method
				 + "' Exception:'" + extraMsg + "'");
		}

		public virtual void Unauthenticated(string remoteHost, string method, string url, 
			string extraMsg)
		{
			Op(KMSAudit.OpStatus.Unauthenticated, null, null, null, "RemoteHost:" + remoteHost
				 + " Method:" + method + " URL:" + url + " ErrorMsg:'" + extraMsg + "'");
		}

		private static string CreateCacheKey(string user, string key, KMS.KMSOp op)
		{
			return user + "#" + key + "#" + op;
		}

		public virtual void Shutdown()
		{
			executor.ShutdownNow();
		}
	}
}
