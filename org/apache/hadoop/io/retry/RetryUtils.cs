using Sharpen;

namespace org.apache.hadoop.io.retry
{
	public class RetryUtils
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.RetryUtils
			)));

		/// <summary>Return the default retry policy set in conf.</summary>
		/// <remarks>
		/// Return the default retry policy set in conf.
		/// If the value retryPolicyEnabledKey is set to false in conf,
		/// use TRY_ONCE_THEN_FAIL.
		/// Otherwise, get the MultipleLinearRandomRetry policy specified in the conf
		/// and then
		/// (1) use multipleLinearRandomRetry for
		/// - remoteExceptionToRetry, or
		/// - IOException other than RemoteException, or
		/// - ServiceException; and
		/// (2) use TRY_ONCE_THEN_FAIL for
		/// - non-remoteExceptionToRetry RemoteException, or
		/// - non-IOException.
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="retryPolicyEnabledKey">conf property key for enabling retry</param>
		/// <param name="defaultRetryPolicyEnabled">default retryPolicyEnabledKey conf value</param>
		/// <param name="retryPolicySpecKey">conf property key for retry policy spec</param>
		/// <param name="defaultRetryPolicySpec">default retryPolicySpecKey conf value</param>
		/// <param name="remoteExceptionToRetry">The particular RemoteException to retry</param>
		/// <returns>the default retry policy.</returns>
		public static org.apache.hadoop.io.retry.RetryPolicy getDefaultRetryPolicy(org.apache.hadoop.conf.Configuration
			 conf, string retryPolicyEnabledKey, bool defaultRetryPolicyEnabled, string retryPolicySpecKey
			, string defaultRetryPolicySpec, java.lang.Class remoteExceptionToRetry)
		{
			org.apache.hadoop.io.retry.RetryPolicy multipleLinearRandomRetry = getMultipleLinearRandomRetry
				(conf, retryPolicyEnabledKey, defaultRetryPolicyEnabled, retryPolicySpecKey, defaultRetryPolicySpec
				);
			if (LOG.isDebugEnabled())
			{
				LOG.debug("multipleLinearRandomRetry = " + multipleLinearRandomRetry);
			}
			if (multipleLinearRandomRetry == null)
			{
				//no retry
				return org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
			}
			else
			{
				return new _RetryPolicy_82(multipleLinearRandomRetry, remoteExceptionToRetry);
			}
		}

		private sealed class _RetryPolicy_82 : org.apache.hadoop.io.retry.RetryPolicy
		{
			public _RetryPolicy_82(org.apache.hadoop.io.retry.RetryPolicy multipleLinearRandomRetry
				, java.lang.Class remoteExceptionToRetry)
			{
				this.multipleLinearRandomRetry = multipleLinearRandomRetry;
				this.remoteExceptionToRetry = remoteExceptionToRetry;
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isMethodIdempotent)
			{
				if (e is com.google.protobuf.ServiceException)
				{
					//unwrap ServiceException
					System.Exception cause = e.InnerException;
					if (cause != null && cause is System.Exception)
					{
						e = (System.Exception)cause;
					}
				}
				//see (1) and (2) in the javadoc of this method.
				org.apache.hadoop.io.retry.RetryPolicy p;
				if (e is org.apache.hadoop.ipc.RetriableException || org.apache.hadoop.io.retry.RetryPolicies
					.getWrappedRetriableException(e) != null)
				{
					// RetriableException or RetriableException wrapped
					p = multipleLinearRandomRetry;
				}
				else
				{
					if (e is org.apache.hadoop.ipc.RemoteException)
					{
						org.apache.hadoop.ipc.RemoteException re = (org.apache.hadoop.ipc.RemoteException
							)e;
						p = remoteExceptionToRetry.getName().Equals(re.getClassName()) ? multipleLinearRandomRetry
							 : org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
					}
					else
					{
						if (e is System.IO.IOException || e is com.google.protobuf.ServiceException)
						{
							p = multipleLinearRandomRetry;
						}
						else
						{
							//non-IOException
							p = org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL;
						}
					}
				}
				if (org.apache.hadoop.io.retry.RetryUtils.LOG.isDebugEnabled())
				{
					org.apache.hadoop.io.retry.RetryUtils.LOG.debug("RETRY " + retries + ") policy=" 
						+ Sharpen.Runtime.getClassForObject(p).getSimpleName() + ", exception=" + e);
				}
				return p.shouldRetry(e, retries, failovers, isMethodIdempotent);
			}

			public override string ToString()
			{
				return "RetryPolicy[" + multipleLinearRandomRetry + ", " + Sharpen.Runtime.getClassForObject
					(org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL).getSimpleName() + 
					"]";
			}

			private readonly org.apache.hadoop.io.retry.RetryPolicy multipleLinearRandomRetry;

			private readonly java.lang.Class remoteExceptionToRetry;
		}

		/// <summary>
		/// Return the MultipleLinearRandomRetry policy specified in the conf,
		/// or null if the feature is disabled.
		/// </summary>
		/// <remarks>
		/// Return the MultipleLinearRandomRetry policy specified in the conf,
		/// or null if the feature is disabled.
		/// If the policy is specified in the conf but the policy cannot be parsed,
		/// the default policy is returned.
		/// Retry policy spec:
		/// N pairs of sleep-time and number-of-retries "s1,n1,s2,n2,..."
		/// </remarks>
		/// <param name="conf"/>
		/// <param name="retryPolicyEnabledKey">conf property key for enabling retry</param>
		/// <param name="defaultRetryPolicyEnabled">default retryPolicyEnabledKey conf value</param>
		/// <param name="retryPolicySpecKey">conf property key for retry policy spec</param>
		/// <param name="defaultRetryPolicySpec">default retryPolicySpecKey conf value</param>
		/// <returns>
		/// the MultipleLinearRandomRetry policy specified in the conf,
		/// or null if the feature is disabled.
		/// </returns>
		public static org.apache.hadoop.io.retry.RetryPolicy getMultipleLinearRandomRetry
			(org.apache.hadoop.conf.Configuration conf, string retryPolicyEnabledKey, bool defaultRetryPolicyEnabled
			, string retryPolicySpecKey, string defaultRetryPolicySpec)
		{
			bool enabled = conf.getBoolean(retryPolicyEnabledKey, defaultRetryPolicyEnabled);
			if (!enabled)
			{
				return null;
			}
			string policy = conf.get(retryPolicySpecKey, defaultRetryPolicySpec);
			org.apache.hadoop.io.retry.RetryPolicy r = org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry
				.parseCommaSeparatedString(policy);
			return (r != null) ? r : org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry
				.parseCommaSeparatedString(defaultRetryPolicySpec);
		}
	}
}
