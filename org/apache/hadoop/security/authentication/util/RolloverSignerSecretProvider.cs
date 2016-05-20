using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>
	/// An abstract SignerSecretProvider that can be use used as the base for a
	/// rolling secret.
	/// </summary>
	/// <remarks>
	/// An abstract SignerSecretProvider that can be use used as the base for a
	/// rolling secret.  The secret will roll over at the same interval as the token
	/// validity, so there are only ever a maximum of two valid secrets at any
	/// given time.  This class handles storing and returning the secrets, as well
	/// as the rolling over.  At a minimum, subclasses simply need to implement the
	/// generateNewSecret() method.  More advanced implementations can override
	/// other methods to provide more advanced behavior, but should be careful when
	/// doing so.
	/// </remarks>
	public abstract class RolloverSignerSecretProvider : org.apache.hadoop.security.authentication.util.SignerSecretProvider
	{
		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.security.authentication.util.RolloverSignerSecretProvider
			)));

		/// <summary>Stores the currently valid secrets.</summary>
		/// <remarks>
		/// Stores the currently valid secrets.  The current secret is the 0th element
		/// in the array.
		/// </remarks>
		private volatile byte[][] secrets;

		private java.util.concurrent.ScheduledExecutorService scheduler;

		private bool schedulerRunning;

		private bool isDestroyed;

		public RolloverSignerSecretProvider()
		{
			schedulerRunning = false;
			isDestroyed = false;
		}

		/// <summary>Initialize the SignerSecretProvider.</summary>
		/// <remarks>
		/// Initialize the SignerSecretProvider.  It initializes the current secret
		/// and starts the scheduler for the rollover to run at an interval of
		/// tokenValidity.
		/// </remarks>
		/// <param name="config">configuration properties</param>
		/// <param name="servletContext">servlet context</param>
		/// <param name="tokenValidity">The amount of time a token is valid for</param>
		/// <exception cref="System.Exception"/>
		public override void init(java.util.Properties config, javax.servlet.ServletContext
			 servletContext, long tokenValidity)
		{
			initSecrets(generateNewSecret(), null);
			startScheduler(tokenValidity, tokenValidity);
		}

		/// <summary>Initializes the secrets array.</summary>
		/// <remarks>
		/// Initializes the secrets array.  This should typically be called only once,
		/// during init but some implementations may wish to call it other times.
		/// previousSecret can be null if there isn't a previous secret, but
		/// currentSecret should never be null.
		/// </remarks>
		/// <param name="currentSecret">The current secret</param>
		/// <param name="previousSecret">The previous secret</param>
		protected internal virtual void initSecrets(byte[] currentSecret, byte[] previousSecret
			)
		{
			secrets = new byte[][] { currentSecret, previousSecret };
		}

		/// <summary>Starts the scheduler for the rollover to run at an interval.</summary>
		/// <param name="initialDelay">The initial delay in the rollover in milliseconds</param>
		/// <param name="period">The interval for the rollover in milliseconds</param>
		protected internal virtual void startScheduler(long initialDelay, long period)
		{
			lock (this)
			{
				if (!schedulerRunning)
				{
					schedulerRunning = true;
					scheduler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor();
					scheduler.scheduleAtFixedRate(new _Runnable_94(this), initialDelay, period, java.util.concurrent.TimeUnit
						.MILLISECONDS);
				}
			}
		}

		private sealed class _Runnable_94 : java.lang.Runnable
		{
			public _Runnable_94(RolloverSignerSecretProvider _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void run()
			{
				this._enclosing.rollSecret();
			}

			private readonly RolloverSignerSecretProvider _enclosing;
		}

		public override void destroy()
		{
			lock (this)
			{
				if (!isDestroyed)
				{
					isDestroyed = true;
					if (scheduler != null)
					{
						scheduler.shutdown();
					}
					schedulerRunning = false;
					base.destroy();
				}
			}
		}

		/// <summary>Rolls the secret.</summary>
		/// <remarks>Rolls the secret.  It is called automatically at the rollover interval.</remarks>
		protected internal virtual void rollSecret()
		{
			lock (this)
			{
				if (!isDestroyed)
				{
					LOG.debug("rolling secret");
					byte[] newSecret = generateNewSecret();
					secrets = new byte[][] { newSecret, secrets[0] };
				}
			}
		}

		/// <summary>Subclasses should implement this to return a new secret.</summary>
		/// <remarks>
		/// Subclasses should implement this to return a new secret.  It will be called
		/// automatically at the secret rollover interval. It should never return null.
		/// </remarks>
		/// <returns>a new secret</returns>
		protected internal abstract byte[] generateNewSecret();

		public override byte[] getCurrentSecret()
		{
			return secrets[0];
		}

		public override byte[][] getAllSecrets()
		{
			return secrets;
		}
	}
}
