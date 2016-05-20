using Sharpen;

namespace org.apache.hadoop.io.retry
{
	/// <summary>Test the behavior of the default retry policy.</summary>
	public class TestDefaultRetryPolicy
	{
		[NUnit.Framework.Rule]
		public NUnit.Framework.rules.Timeout timeout = new NUnit.Framework.rules.Timeout(
			300000);

		/// <summary>
		/// Verify that the default retry policy correctly retries
		/// RetriableException when defaultRetryPolicyEnabled is enabled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithRetriable()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.retry.RetryPolicy policy = org.apache.hadoop.io.retry.RetryUtils
				.getDefaultRetryPolicy(conf, "Test.No.Such.Key", true, "Test.No.Such.Key", "10000,6"
				, null);
			// defaultRetryPolicyEnabled = true
			org.apache.hadoop.io.retry.RetryPolicy.RetryAction action = policy.shouldRetry(new 
				org.apache.hadoop.ipc.RetriableException("Dummy exception"), 0, 0, true);
			NUnit.Framework.Assert.assertThat(action.action, org.hamcrest.core.Is.@is(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
				.RETRY));
		}

		/// <summary>
		/// Verify that the default retry policy correctly retries
		/// a RetriableException wrapped in a RemoteException when
		/// defaultRetryPolicyEnabled is enabled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithWrappedRetriable()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.retry.RetryPolicy policy = org.apache.hadoop.io.retry.RetryUtils
				.getDefaultRetryPolicy(conf, "Test.No.Such.Key", true, "Test.No.Such.Key", "10000,6"
				, null);
			// defaultRetryPolicyEnabled = true
			org.apache.hadoop.io.retry.RetryPolicy.RetryAction action = policy.shouldRetry(new 
				org.apache.hadoop.ipc.RemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RetriableException
				)).getName(), "Dummy exception"), 0, 0, true);
			NUnit.Framework.Assert.assertThat(action.action, org.hamcrest.core.Is.@is(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
				.RETRY));
		}

		/// <summary>
		/// Verify that the default retry policy does *not* retry
		/// RetriableException when defaultRetryPolicyEnabled is disabled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWithRetriableAndRetryDisabled()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.io.retry.RetryPolicy policy = org.apache.hadoop.io.retry.RetryUtils
				.getDefaultRetryPolicy(conf, "Test.No.Such.Key", false, "Test.No.Such.Key", "10000,6"
				, null);
			// defaultRetryPolicyEnabled = false
			org.apache.hadoop.io.retry.RetryPolicy.RetryAction action = policy.shouldRetry(new 
				org.apache.hadoop.ipc.RetriableException("Dummy exception"), 0, 0, true);
			NUnit.Framework.Assert.assertThat(action.action, org.hamcrest.core.Is.@is(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
				.FAIL));
		}
	}
}
