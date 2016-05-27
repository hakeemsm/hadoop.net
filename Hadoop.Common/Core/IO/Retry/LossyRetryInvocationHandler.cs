using System.Reflection;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Retry
{
	/// <summary>A dummy invocation handler extending RetryInvocationHandler.</summary>
	/// <remarks>
	/// A dummy invocation handler extending RetryInvocationHandler. It drops the
	/// first N number of responses. This invocation handler is only used for testing.
	/// </remarks>
	public class LossyRetryInvocationHandler<T> : RetryInvocationHandler<T>
	{
		private readonly int numToDrop;

		private static readonly ThreadLocal<int> RetryCount = new ThreadLocal<int>();

		public LossyRetryInvocationHandler(int numToDrop, FailoverProxyProvider<T> proxyProvider
			, RetryPolicy retryPolicy)
			: base(proxyProvider, retryPolicy)
		{
			this.numToDrop = numToDrop;
		}

		/// <exception cref="System.Exception"/>
		public override object Invoke(object proxy, MethodInfo method, object[] args)
		{
			RetryCount.Set(0);
			return base.Invoke(proxy, method, args);
		}

		/// <exception cref="System.Exception"/>
		protected internal override object InvokeMethod(MethodInfo method, object[] args)
		{
			object result = base.InvokeMethod(method, args);
			int retryCount = RetryCount.Get();
			if (retryCount < this.numToDrop)
			{
				RetryCount.Set(++retryCount);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Drop the response. Current retryCount == " + retryCount);
				}
				throw new RetriableException("Fake Exception");
			}
			else
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("retryCount == " + retryCount + ". It's time to normally process the response"
						);
				}
				return result;
			}
		}
	}
}
