using Javax.Servlet;
using Org.Slf4j;


namespace Org.Apache.Hadoop.Security.Authentication.Util
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
	public abstract class RolloverSignerSecretProvider : SignerSecretProvider
	{
		private static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Security.Authentication.Util.RolloverSignerSecretProvider
			));

		/// <summary>Stores the currently valid secrets.</summary>
		/// <remarks>
		/// Stores the currently valid secrets.  The current secret is the 0th element
		/// in the array.
		/// </remarks>
		private volatile byte[][] secrets;

		private ScheduledExecutorService scheduler;

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
		public override void Init(Properties config, ServletContext servletContext, long 
			tokenValidity)
		{
			InitSecrets(GenerateNewSecret(), null);
			StartScheduler(tokenValidity, tokenValidity);
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
		protected internal virtual void InitSecrets(byte[] currentSecret, byte[] previousSecret
			)
		{
			secrets = new byte[][] { currentSecret, previousSecret };
		}

		/// <summary>Starts the scheduler for the rollover to run at an interval.</summary>
		/// <param name="initialDelay">The initial delay in the rollover in milliseconds</param>
		/// <param name="period">The interval for the rollover in milliseconds</param>
		protected internal virtual void StartScheduler(long initialDelay, long period)
		{
			lock (this)
			{
				if (!schedulerRunning)
				{
					schedulerRunning = true;
					scheduler = Executors.NewSingleThreadScheduledExecutor();
					scheduler.ScheduleAtFixedRate(new _Runnable_94(this), initialDelay, period, TimeUnit
						.Milliseconds);
				}
			}
		}

		private sealed class _Runnable_94 : Runnable
		{
			public _Runnable_94(RolloverSignerSecretProvider _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				this._enclosing.RollSecret();
			}

			private readonly RolloverSignerSecretProvider _enclosing;
		}

		public override void Destroy()
		{
			lock (this)
			{
				if (!isDestroyed)
				{
					isDestroyed = true;
					if (scheduler != null)
					{
						scheduler.Shutdown();
					}
					schedulerRunning = false;
					base.Destroy();
				}
			}
		}

		/// <summary>Rolls the secret.</summary>
		/// <remarks>Rolls the secret.  It is called automatically at the rollover interval.</remarks>
		protected internal virtual void RollSecret()
		{
			lock (this)
			{
				if (!isDestroyed)
				{
					Log.Debug("rolling secret");
					byte[] newSecret = GenerateNewSecret();
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
		protected internal abstract byte[] GenerateNewSecret();

		public override byte[] GetCurrentSecret()
		{
			return secrets[0];
		}

		public override byte[][] GetAllSecrets()
		{
			return secrets;
		}
	}
}
