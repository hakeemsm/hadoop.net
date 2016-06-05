using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	public class TestQuorumCall
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestQuorums()
		{
			IDictionary<string, SettableFuture<string>> futures = ImmutableMap.Of("f1", SettableFuture
				.Create<string>(), "f2", SettableFuture.Create<string>(), "f3", SettableFuture.Create
				<string>());
			QuorumCall<string, string> q = QuorumCall.Create(futures);
			NUnit.Framework.Assert.AreEqual(0, q.CountResponses());
			futures["f1"].Set("first future");
			q.WaitFor(1, 0, 0, 100000, "test");
			// wait for 1 response
			q.WaitFor(0, 1, 0, 100000, "test");
			// wait for 1 success
			NUnit.Framework.Assert.AreEqual(1, q.CountResponses());
			futures["f2"].SetException(new Exception("error"));
			NUnit.Framework.Assert.AreEqual(2, q.CountResponses());
			futures["f3"].Set("second future");
			q.WaitFor(3, 0, 100, 100000, "test");
			// wait for 3 responses
			q.WaitFor(0, 2, 100, 100000, "test");
			// 2 successes
			NUnit.Framework.Assert.AreEqual(3, q.CountResponses());
			NUnit.Framework.Assert.AreEqual("f1=first future,f3=second future", Joiner.On(","
				).WithKeyValueSeparator("=").Join(new SortedDictionary<string, string>(q.GetResults
				())));
			try
			{
				q.WaitFor(0, 4, 100, 10, "test");
				NUnit.Framework.Assert.Fail("Didn't time out waiting for more responses than came back"
					);
			}
			catch (TimeoutException)
			{
			}
		}
		// expected
	}
}
