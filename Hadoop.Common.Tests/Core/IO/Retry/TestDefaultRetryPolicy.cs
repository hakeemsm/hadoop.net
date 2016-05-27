using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Retry
{
	/// <summary>Test the behavior of the default retry policy.</summary>
	public class TestDefaultRetryPolicy
	{
		[Rule]
		public Timeout timeout = new Timeout(300000);

		/// <summary>
		/// Verify that the default retry policy correctly retries
		/// RetriableException when defaultRetryPolicyEnabled is enabled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithRetriable()
		{
			Configuration conf = new Configuration();
			RetryPolicy policy = RetryUtils.GetDefaultRetryPolicy(conf, "Test.No.Such.Key", true
				, "Test.No.Such.Key", "10000,6", null);
			// defaultRetryPolicyEnabled = true
			RetryPolicy.RetryAction action = policy.ShouldRetry(new RetriableException("Dummy exception"
				), 0, 0, true);
			Assert.AssertThat(action.action, IS.Is(RetryPolicy.RetryAction.RetryDecision.Retry
				));
		}

		/// <summary>
		/// Verify that the default retry policy correctly retries
		/// a RetriableException wrapped in a RemoteException when
		/// defaultRetryPolicyEnabled is enabled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithWrappedRetriable()
		{
			Configuration conf = new Configuration();
			RetryPolicy policy = RetryUtils.GetDefaultRetryPolicy(conf, "Test.No.Such.Key", true
				, "Test.No.Such.Key", "10000,6", null);
			// defaultRetryPolicyEnabled = true
			RetryPolicy.RetryAction action = policy.ShouldRetry(new RemoteException(typeof(RetriableException
				).FullName, "Dummy exception"), 0, 0, true);
			Assert.AssertThat(action.action, IS.Is(RetryPolicy.RetryAction.RetryDecision.Retry
				));
		}

		/// <summary>
		/// Verify that the default retry policy does *not* retry
		/// RetriableException when defaultRetryPolicyEnabled is disabled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithRetriableAndRetryDisabled()
		{
			Configuration conf = new Configuration();
			RetryPolicy policy = RetryUtils.GetDefaultRetryPolicy(conf, "Test.No.Such.Key", false
				, "Test.No.Such.Key", "10000,6", null);
			// defaultRetryPolicyEnabled = false
			RetryPolicy.RetryAction action = policy.ShouldRetry(new RetriableException("Dummy exception"
				), 0, 0, true);
			Assert.AssertThat(action.action, IS.Is(RetryPolicy.RetryAction.RetryDecision.Fail
				));
		}
	}
}
