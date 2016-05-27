using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>A daemon thread that waits for the next file system to renew.</summary>
	public class DelegationTokenRenewer : Sharpen.Thread
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.DelegationTokenRenewer
			));

		/// <summary>The renewable interface used by the renewer.</summary>
		public interface Renewable
		{
			/// <returns>the renew token.</returns>
			Org.Apache.Hadoop.Security.Token.Token<object> GetRenewToken();

			/// <summary>Set delegation token.</summary>
			void SetDelegationToken<T>(Org.Apache.Hadoop.Security.Token.Token<T> token)
				where T : TokenIdentifier;
		}

		/// <summary>
		/// An action that will renew and replace the file system's delegation
		/// tokens automatically.
		/// </summary>
		public class RenewAction<T> : Delayed
			where T : FileSystem
		{
			/// <summary>when should the renew happen</summary>
			private long renewalTime;

			/// <summary>a weak reference to the file system so that it can be garbage collected</summary>
			private readonly WeakReference<T> weakFs;

			private Org.Apache.Hadoop.Security.Token.Token<object> token;

			internal bool isValid = true;

			private RenewAction(T fs)
			{
				this.weakFs = new WeakReference<T>(fs);
				this.token = fs.GetRenewToken();
				UpdateRenewalTime(renewCycle);
			}

			public virtual bool IsValid()
			{
				return isValid;
			}

			/// <summary>Get the delay until this event should happen.</summary>
			public virtual long GetDelay(TimeUnit unit)
			{
				long millisLeft = renewalTime - Time.Now();
				return unit.Convert(millisLeft, TimeUnit.Milliseconds);
			}

			public virtual int CompareTo(Delayed delayed)
			{
				DelegationTokenRenewer.RenewAction<object> that = (DelegationTokenRenewer.RenewAction
					<object>)delayed;
				return this.renewalTime < that.renewalTime ? -1 : this.renewalTime == that.renewalTime
					 ? 0 : 1;
			}

			public override int GetHashCode()
			{
				return token.GetHashCode();
			}

			public override bool Equals(object that)
			{
				if (this == that)
				{
					return true;
				}
				else
				{
					if (that == null || !(that is DelegationTokenRenewer.RenewAction))
					{
						return false;
					}
				}
				return token.Equals(((DelegationTokenRenewer.RenewAction<object>)that).token);
			}

			/// <summary>Set a new time for the renewal.</summary>
			/// <remarks>
			/// Set a new time for the renewal.
			/// It can only be called when the action is not in the queue or any
			/// collection because the hashCode may change
			/// </remarks>
			/// <param name="newTime">the new time</param>
			private void UpdateRenewalTime(long delay)
			{
				renewalTime = Time.Now() + delay - delay / 10;
			}

			/// <summary>Renew or replace the delegation token for this file system.</summary>
			/// <remarks>
			/// Renew or replace the delegation token for this file system.
			/// It can only be called when the action is not in the queue.
			/// </remarks>
			/// <returns/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private bool Renew()
			{
				T fs = weakFs.Get();
				bool b = fs != null;
				if (b)
				{
					lock (fs)
					{
						try
						{
							long expires = token.Renew(fs.GetConf());
							UpdateRenewalTime(expires - Time.Now());
						}
						catch (IOException ie)
						{
							try
							{
								Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = fs.AddDelegationTokens(
									null, null);
								if (tokens.Length == 0)
								{
									throw new IOException("addDelegationTokens returned no tokens");
								}
								token = tokens[0];
								UpdateRenewalTime(renewCycle);
								fs.SetDelegationToken(token);
							}
							catch (IOException)
							{
								isValid = false;
								throw new IOException("Can't renew or get new delegation token ", ie);
							}
						}
					}
				}
				return b;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void Cancel()
			{
				T fs = weakFs.Get();
				if (fs != null)
				{
					token.Cancel(fs.GetConf());
				}
			}

			public override string ToString()
			{
				DelegationTokenRenewer.Renewable fs = weakFs.Get();
				return fs == null ? "evaporated token renew" : "The token will be renewed in " + 
					GetDelay(TimeUnit.Seconds) + " secs, renewToken=" + token;
			}
		}

		/// <summary>assumes renew cycle for a token is 24 hours...</summary>
		private const long RenewCycle = 24 * 60 * 60 * 1000;

		[InterfaceAudience.Private]
		[VisibleForTesting]
		public static long renewCycle = RenewCycle;

		/// <summary>
		/// Queue to maintain the RenewActions to be processed by the
		/// <see cref="Run()"/>
		/// 
		/// </summary>
		private volatile DelayQueue<DelegationTokenRenewer.RenewAction<object>> queue = new 
			DelayQueue<DelegationTokenRenewer.RenewAction<object>>();

		/// <summary>For testing purposes</summary>
		[VisibleForTesting]
		protected internal virtual int GetRenewQueueLength()
		{
			return queue.Count;
		}

		/// <summary>Create the singleton instance.</summary>
		/// <remarks>
		/// Create the singleton instance. However, the thread can be started lazily in
		/// <see cref="AddRenewAction{T}(FileSystem)"/>
		/// </remarks>
		private static DelegationTokenRenewer Instance = null;

		private DelegationTokenRenewer(Type clazz)
			: base(clazz.Name + "-" + typeof(DelegationTokenRenewer).Name)
		{
			SetDaemon(true);
		}

		public static DelegationTokenRenewer GetInstance()
		{
			lock (typeof(DelegationTokenRenewer))
			{
				if (Instance == null)
				{
					Instance = new DelegationTokenRenewer(typeof(FileSystem));
				}
				return Instance;
			}
		}

		[VisibleForTesting]
		internal static void Reset()
		{
			lock (typeof(DelegationTokenRenewer))
			{
				if (Instance != null)
				{
					Instance.queue.Clear();
					Instance.Interrupt();
					try
					{
						Instance.Join();
					}
					catch (Exception)
					{
						Log.Warn("Failed to reset renewer");
					}
					finally
					{
						Instance = null;
					}
				}
			}
		}

		/// <summary>Add a renew action to the queue.</summary>
		public virtual DelegationTokenRenewer.RenewAction<T> AddRenewAction<T>(T fs)
			where T : FileSystem
		{
			lock (this)
			{
				if (!IsAlive())
				{
					Start();
				}
			}
			DelegationTokenRenewer.RenewAction<T> action = new DelegationTokenRenewer.RenewAction
				<T>(fs);
			if (action.token != null)
			{
				queue.AddItem(action);
			}
			else
			{
				fs.Log.Error("does not have a token for renewal");
			}
			return action;
		}

		/// <summary>Remove the associated renew action from the queue</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveRenewAction<T>(T fs)
			where T : FileSystem
		{
			DelegationTokenRenewer.RenewAction<T> action = new DelegationTokenRenewer.RenewAction
				<T>(fs);
			if (queue.Remove(action))
			{
				try
				{
					action.Cancel();
				}
				catch (Exception ie)
				{
					Log.Error("Interrupted while canceling token for " + fs.GetUri() + "filesystem");
					if (Log.IsDebugEnabled())
					{
						Log.Debug(ie.GetStackTrace());
					}
				}
			}
		}

		public override void Run()
		{
			for (; ; )
			{
				DelegationTokenRenewer.RenewAction<object> action = null;
				try
				{
					action = queue.Take();
					if (action.Renew())
					{
						queue.AddItem(action);
					}
				}
				catch (Exception)
				{
					return;
				}
				catch (Exception ie)
				{
					action.weakFs.Get().Log.Warn("Failed to renew token, action=" + action, ie);
				}
			}
		}
	}
}
