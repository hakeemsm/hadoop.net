using System;
using System.IO;
using Com.Google.Protobuf;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Retry
{
	public class RetryUtils
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(RetryUtils));

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
		public static RetryPolicy GetDefaultRetryPolicy(Configuration conf, string retryPolicyEnabledKey
			, bool defaultRetryPolicyEnabled, string retryPolicySpecKey, string defaultRetryPolicySpec
			, Type remoteExceptionToRetry)
		{
			RetryPolicy multipleLinearRandomRetry = GetMultipleLinearRandomRetry(conf, retryPolicyEnabledKey
				, defaultRetryPolicyEnabled, retryPolicySpecKey, defaultRetryPolicySpec);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("multipleLinearRandomRetry = " + multipleLinearRandomRetry);
			}
			if (multipleLinearRandomRetry == null)
			{
				//no retry
				return RetryPolicies.TryOnceThenFail;
			}
			else
			{
				return new _RetryPolicy_82(multipleLinearRandomRetry, remoteExceptionToRetry);
			}
		}

		private sealed class _RetryPolicy_82 : RetryPolicy
		{
			public _RetryPolicy_82(RetryPolicy multipleLinearRandomRetry, Type remoteExceptionToRetry
				)
			{
				this.multipleLinearRandomRetry = multipleLinearRandomRetry;
				this.remoteExceptionToRetry = remoteExceptionToRetry;
			}

			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isMethodIdempotent)
			{
				if (e is ServiceException)
				{
					//unwrap ServiceException
					Exception cause = e.InnerException;
					if (cause != null && cause is Exception)
					{
						e = (Exception)cause;
					}
				}
				//see (1) and (2) in the javadoc of this method.
				RetryPolicy p;
				if (e is RetriableException || RetryPolicies.GetWrappedRetriableException(e) != null)
				{
					// RetriableException or RetriableException wrapped
					p = multipleLinearRandomRetry;
				}
				else
				{
					if (e is RemoteException)
					{
						RemoteException re = (RemoteException)e;
						p = remoteExceptionToRetry.FullName.Equals(re.GetClassName()) ? multipleLinearRandomRetry
							 : RetryPolicies.TryOnceThenFail;
					}
					else
					{
						if (e is IOException || e is ServiceException)
						{
							p = multipleLinearRandomRetry;
						}
						else
						{
							//non-IOException
							p = RetryPolicies.TryOnceThenFail;
						}
					}
				}
				if (RetryUtils.Log.IsDebugEnabled())
				{
					RetryUtils.Log.Debug("RETRY " + retries + ") policy=" + p.GetType().Name + ", exception="
						 + e);
				}
				return p.ShouldRetry(e, retries, failovers, isMethodIdempotent);
			}

			public override string ToString()
			{
				return "RetryPolicy[" + multipleLinearRandomRetry + ", " + RetryPolicies.TryOnceThenFail
					.GetType().Name + "]";
			}

			private readonly RetryPolicy multipleLinearRandomRetry;

			private readonly Type remoteExceptionToRetry;
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
		public static RetryPolicy GetMultipleLinearRandomRetry(Configuration conf, string
			 retryPolicyEnabledKey, bool defaultRetryPolicyEnabled, string retryPolicySpecKey
			, string defaultRetryPolicySpec)
		{
			bool enabled = conf.GetBoolean(retryPolicyEnabledKey, defaultRetryPolicyEnabled);
			if (!enabled)
			{
				return null;
			}
			string policy = conf.Get(retryPolicySpecKey, defaultRetryPolicySpec);
			RetryPolicy r = RetryPolicies.MultipleLinearRandomRetry.ParseCommaSeparatedString
				(policy);
			return (r != null) ? r : RetryPolicies.MultipleLinearRandomRetry.ParseCommaSeparatedString
				(defaultRetryPolicySpec);
		}
	}
}
