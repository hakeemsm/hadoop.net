using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class ServerProxy
	{
		protected internal static RetryPolicy CreateRetryPolicy(Configuration conf, string
			 maxWaitTimeStr, long defMaxWaitTime, string connectRetryIntervalStr, long defRetryInterval
			)
		{
			long maxWaitTime = conf.GetLong(maxWaitTimeStr, defMaxWaitTime);
			long retryIntervalMS = conf.GetLong(connectRetryIntervalStr, defRetryInterval);
			if (maxWaitTime == -1)
			{
				// wait forever.
				return RetryPolicies.RetryForever;
			}
			Preconditions.CheckArgument(maxWaitTime > 0, "Invalid Configuration. " + maxWaitTimeStr
				 + " should be a positive value.");
			Preconditions.CheckArgument(retryIntervalMS > 0, "Invalid Configuration. " + connectRetryIntervalStr
				 + "should be a positive value.");
			RetryPolicy retryPolicy = RetryPolicies.RetryUpToMaximumTimeWithFixedSleep(maxWaitTime
				, retryIntervalMS, TimeUnit.Milliseconds);
			IDictionary<Type, RetryPolicy> exceptionToPolicyMap = new Dictionary<Type, RetryPolicy
				>();
			exceptionToPolicyMap[typeof(EOFException)] = retryPolicy;
			exceptionToPolicyMap[typeof(ConnectException)] = retryPolicy;
			exceptionToPolicyMap[typeof(NoRouteToHostException)] = retryPolicy;
			exceptionToPolicyMap[typeof(UnknownHostException)] = retryPolicy;
			exceptionToPolicyMap[typeof(RetriableException)] = retryPolicy;
			exceptionToPolicyMap[typeof(SocketException)] = retryPolicy;
			exceptionToPolicyMap[typeof(NMNotYetReadyException)] = retryPolicy;
			return RetryPolicies.RetryByException(RetryPolicies.TryOnceThenFail, exceptionToPolicyMap
				);
		}

		protected internal static T CreateRetriableProxy<T>(Configuration conf, UserGroupInformation
			 user, YarnRPC rpc, IPEndPoint serverAddress, RetryPolicy retryPolicy)
		{
			System.Type protocol = typeof(T);
			T proxy = user.DoAs(new _PrivilegedAction_89(rpc, protocol, serverAddress, conf));
			return (T)RetryProxy.Create(protocol, proxy, retryPolicy);
		}

		private sealed class _PrivilegedAction_89 : PrivilegedAction<T>
		{
			public _PrivilegedAction_89(YarnRPC rpc, Type protocol, IPEndPoint serverAddress, 
				Configuration conf)
			{
				this.rpc = rpc;
				this.protocol = protocol;
				this.serverAddress = serverAddress;
				this.conf = conf;
			}

			public T Run()
			{
				return (T)rpc.GetProxy(protocol, serverAddress, conf);
			}

			private readonly YarnRPC rpc;

			private readonly Type protocol;

			private readonly IPEndPoint serverAddress;

			private readonly Configuration conf;
		}
	}
}
