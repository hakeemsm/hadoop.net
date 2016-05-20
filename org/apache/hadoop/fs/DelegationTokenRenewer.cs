using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>A daemon thread that waits for the next file system to renew.</summary>
	public class DelegationTokenRenewer : java.lang.Thread
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.DelegationTokenRenewer
			)));

		/// <summary>The renewable interface used by the renewer.</summary>
		public interface Renewable
		{
			/// <returns>the renew token.</returns>
			org.apache.hadoop.security.token.Token<object> getRenewToken();

			/// <summary>Set delegation token.</summary>
			void setDelegationToken<T>(org.apache.hadoop.security.token.Token<T> token)
				where T : org.apache.hadoop.security.token.TokenIdentifier;
		}

		/// <summary>
		/// An action that will renew and replace the file system's delegation
		/// tokens automatically.
		/// </summary>
		public class RenewAction<T> : java.util.concurrent.Delayed
			where T : org.apache.hadoop.fs.FileSystem
		{
			/// <summary>when should the renew happen</summary>
			private long renewalTime;

			/// <summary>a weak reference to the file system so that it can be garbage collected</summary>
			private readonly java.lang.@ref.WeakReference<T> weakFs;

			private org.apache.hadoop.security.token.Token<object> token;

			internal bool isValid = true;

			private RenewAction(T fs)
			{
				this.weakFs = new java.lang.@ref.WeakReference<T>(fs);
				this.token = fs.getRenewToken();
				updateRenewalTime(renewCycle);
			}

			public virtual bool isValid()
			{
				return isValid;
			}

			/// <summary>Get the delay until this event should happen.</summary>
			public virtual long getDelay(java.util.concurrent.TimeUnit unit)
			{
				long millisLeft = renewalTime - org.apache.hadoop.util.Time.now();
				return unit.convert(millisLeft, java.util.concurrent.TimeUnit.MILLISECONDS);
			}

			public virtual int compareTo(java.util.concurrent.Delayed delayed)
			{
				org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction<object> that = (org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction
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
					if (that == null || !(that is org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction
						))
					{
						return false;
					}
				}
				return token.Equals(((org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction<object
					>)that).token);
			}

			/// <summary>Set a new time for the renewal.</summary>
			/// <remarks>
			/// Set a new time for the renewal.
			/// It can only be called when the action is not in the queue or any
			/// collection because the hashCode may change
			/// </remarks>
			/// <param name="newTime">the new time</param>
			private void updateRenewalTime(long delay)
			{
				renewalTime = org.apache.hadoop.util.Time.now() + delay - delay / 10;
			}

			/// <summary>Renew or replace the delegation token for this file system.</summary>
			/// <remarks>
			/// Renew or replace the delegation token for this file system.
			/// It can only be called when the action is not in the queue.
			/// </remarks>
			/// <returns/>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private bool renew()
			{
				T fs = weakFs.get();
				bool b = fs != null;
				if (b)
				{
					lock (fs)
					{
						try
						{
							long expires = token.renew(fs.getConf());
							updateRenewalTime(expires - org.apache.hadoop.util.Time.now());
						}
						catch (System.IO.IOException ie)
						{
							try
							{
								org.apache.hadoop.security.token.Token<object>[] tokens = fs.addDelegationTokens(
									null, null);
								if (tokens.Length == 0)
								{
									throw new System.IO.IOException("addDelegationTokens returned no tokens");
								}
								token = tokens[0];
								updateRenewalTime(renewCycle);
								fs.setDelegationToken(token);
							}
							catch (System.IO.IOException)
							{
								isValid = false;
								throw new System.IO.IOException("Can't renew or get new delegation token ", ie);
							}
						}
					}
				}
				return b;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			private void cancel()
			{
				T fs = weakFs.get();
				if (fs != null)
				{
					token.cancel(fs.getConf());
				}
			}

			public override string ToString()
			{
				org.apache.hadoop.fs.DelegationTokenRenewer.Renewable fs = weakFs.get();
				return fs == null ? "evaporated token renew" : "The token will be renewed in " + 
					getDelay(java.util.concurrent.TimeUnit.SECONDS) + " secs, renewToken=" + token;
			}
		}

		/// <summary>assumes renew cycle for a token is 24 hours...</summary>
		private const long RENEW_CYCLE = 24 * 60 * 60 * 1000;

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		[com.google.common.annotations.VisibleForTesting]
		public static long renewCycle = RENEW_CYCLE;

		/// <summary>
		/// Queue to maintain the RenewActions to be processed by the
		/// <see cref="run()"/>
		/// 
		/// </summary>
		private volatile java.util.concurrent.DelayQueue<org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction
			<object>> queue = new java.util.concurrent.DelayQueue<org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction
			<object>>();

		/// <summary>For testing purposes</summary>
		[com.google.common.annotations.VisibleForTesting]
		protected internal virtual int getRenewQueueLength()
		{
			return queue.Count;
		}

		/// <summary>Create the singleton instance.</summary>
		/// <remarks>
		/// Create the singleton instance. However, the thread can be started lazily in
		/// <see cref="addRenewAction{T}(FileSystem)"/>
		/// </remarks>
		private static org.apache.hadoop.fs.DelegationTokenRenewer INSTANCE = null;

		private DelegationTokenRenewer(java.lang.Class clazz)
			: base(clazz.getSimpleName() + "-" + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.DelegationTokenRenewer
				)).getSimpleName())
		{
			setDaemon(true);
		}

		public static org.apache.hadoop.fs.DelegationTokenRenewer getInstance()
		{
			lock (typeof(DelegationTokenRenewer))
			{
				if (INSTANCE == null)
				{
					INSTANCE = new org.apache.hadoop.fs.DelegationTokenRenewer(Sharpen.Runtime.getClassForType
						(typeof(org.apache.hadoop.fs.FileSystem)));
				}
				return INSTANCE;
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal static void reset()
		{
			lock (typeof(DelegationTokenRenewer))
			{
				if (INSTANCE != null)
				{
					INSTANCE.queue.clear();
					INSTANCE.interrupt();
					try
					{
						INSTANCE.join();
					}
					catch (System.Exception)
					{
						LOG.warn("Failed to reset renewer");
					}
					finally
					{
						INSTANCE = null;
					}
				}
			}
		}

		/// <summary>Add a renew action to the queue.</summary>
		public virtual org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction<T> addRenewAction
			<T>(T fs)
			where T : org.apache.hadoop.fs.FileSystem
		{
			lock (this)
			{
				if (!isAlive())
				{
					start();
				}
			}
			org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction<T> action = new org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction
				<T>(fs);
			if (action.token != null)
			{
				queue.add(action);
			}
			else
			{
				fs.LOG.error("does not have a token for renewal");
			}
			return action;
		}

		/// <summary>Remove the associated renew action from the queue</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void removeRenewAction<T>(T fs)
			where T : org.apache.hadoop.fs.FileSystem
		{
			org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction<T> action = new org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction
				<T>(fs);
			if (queue.remove(action))
			{
				try
				{
					action.cancel();
				}
				catch (System.Exception ie)
				{
					LOG.error("Interrupted while canceling token for " + fs.getUri() + "filesystem");
					if (LOG.isDebugEnabled())
					{
						LOG.debug(ie.getStackTrace());
					}
				}
			}
		}

		public override void run()
		{
			for (; ; )
			{
				org.apache.hadoop.fs.DelegationTokenRenewer.RenewAction<object> action = null;
				try
				{
					action = queue.take();
					if (action.renew())
					{
						queue.add(action);
					}
				}
				catch (System.Exception)
				{
					return;
				}
				catch (System.Exception ie)
				{
					action.weakFs.get().LOG.warn("Failed to renew token, action=" + action, ie);
				}
			}
		}
	}
}
