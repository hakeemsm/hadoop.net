using Sharpen;

namespace org.apache.hadoop.io.retry
{
	/// <summary>A dummy invocation handler extending RetryInvocationHandler.</summary>
	/// <remarks>
	/// A dummy invocation handler extending RetryInvocationHandler. It drops the
	/// first N number of responses. This invocation handler is only used for testing.
	/// </remarks>
	public class LossyRetryInvocationHandler<T> : org.apache.hadoop.io.retry.RetryInvocationHandler
		<T>
	{
		private readonly int numToDrop;

		private static readonly java.lang.ThreadLocal<int> RetryCount = new java.lang.ThreadLocal
			<int>();

		public LossyRetryInvocationHandler(int numToDrop, org.apache.hadoop.io.retry.FailoverProxyProvider
			<T> proxyProvider, org.apache.hadoop.io.retry.RetryPolicy retryPolicy)
			: base(proxyProvider, retryPolicy)
		{
			this.numToDrop = numToDrop;
		}

		/// <exception cref="System.Exception"/>
		public override object invoke(object proxy, java.lang.reflect.Method method, object
			[] args)
		{
			RetryCount.set(0);
			return base.invoke(proxy, method, args);
		}

		/// <exception cref="System.Exception"/>
		protected internal override object invokeMethod(java.lang.reflect.Method method, 
			object[] args)
		{
			object result = base.invokeMethod(method, args);
			int retryCount = RetryCount.get();
			if (retryCount < this.numToDrop)
			{
				RetryCount.set(++retryCount);
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Drop the response. Current retryCount == " + retryCount);
				}
				throw new org.apache.hadoop.ipc.RetriableException("Fake Exception");
			}
			else
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("retryCount == " + retryCount + ". It's time to normally process the response"
						);
				}
				return result;
			}
		}
	}
}
