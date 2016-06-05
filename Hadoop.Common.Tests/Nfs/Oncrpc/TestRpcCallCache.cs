using System;
using System.Collections.Generic;
using System.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Unit tests for
	/// <see cref="RpcCallCache"/>
	/// </summary>
	public class TestRpcCallCache
	{
		public virtual void TestRpcCallCacheConstructorIllegalArgument0()
		{
			new RpcCallCache("test", 0);
		}

		public virtual void TestRpcCallCacheConstructorIllegalArgumentNegative()
		{
			new RpcCallCache("test", -1);
		}

		[Fact]
		public virtual void TestRpcCallCacheConstructor()
		{
			RpcCallCache cache = new RpcCallCache("test", 100);
			Assert.Equal("test", cache.GetProgram());
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestAddRemoveEntries()
		{
			RpcCallCache cache = new RpcCallCache("test", 100);
			IPAddress clientIp = Sharpen.Extensions.GetAddressByName("1.1.1.1");
			int xid = 100;
			// Ensure null is returned when there is no entry in the cache
			// An entry is added to indicate the request is in progress
			RpcCallCache.CacheEntry e = cache.CheckOrAddToCache(clientIp, xid);
			NUnit.Framework.Assert.IsNull(e);
			e = cache.CheckOrAddToCache(clientIp, xid);
			ValidateInprogressCacheEntry(e);
			// Set call as completed
			RpcResponse response = Org.Mockito.Mockito.Mock<RpcResponse>();
			cache.CallCompleted(clientIp, xid, response);
			e = cache.CheckOrAddToCache(clientIp, xid);
			ValidateCompletedCacheEntry(e, response);
		}

		private void ValidateInprogressCacheEntry(RpcCallCache.CacheEntry c)
		{
			Assert.True(c.IsInProgress());
			NUnit.Framework.Assert.IsFalse(c.IsCompleted());
			NUnit.Framework.Assert.IsNull(c.GetResponse());
		}

		private void ValidateCompletedCacheEntry(RpcCallCache.CacheEntry c, RpcResponse response
			)
		{
			NUnit.Framework.Assert.IsFalse(c.IsInProgress());
			Assert.True(c.IsCompleted());
			Assert.Equal(response, c.GetResponse());
		}

		[Fact]
		public virtual void TestCacheEntry()
		{
			RpcCallCache.CacheEntry c = new RpcCallCache.CacheEntry();
			ValidateInprogressCacheEntry(c);
			Assert.True(c.IsInProgress());
			NUnit.Framework.Assert.IsFalse(c.IsCompleted());
			NUnit.Framework.Assert.IsNull(c.GetResponse());
			RpcResponse response = Org.Mockito.Mockito.Mock<RpcResponse>();
			c.SetResponse(response);
			ValidateCompletedCacheEntry(c, response);
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestCacheFunctionality()
		{
			RpcCallCache cache = new RpcCallCache("Test", 10);
			// Add 20 entries to the cache and only last 10 should be retained
			int size = 0;
			for (int clientId = 0; clientId < 20; clientId++)
			{
				IPAddress clientIp = Sharpen.Extensions.GetAddressByName("1.1.1." + clientId);
				System.Console.Out.WriteLine("Adding " + clientIp);
				cache.CheckOrAddToCache(clientIp, 0);
				size = Math.Min(++size, 10);
				System.Console.Out.WriteLine("Cache size " + cache.Size());
				Assert.Equal(size, cache.Size());
				// Ensure the cache size is correct
				// Ensure the cache entries are correct
				int startEntry = Math.Max(clientId - 10 + 1, 0);
				IEnumerator<KeyValuePair<RpcCallCache.ClientRequest, RpcCallCache.CacheEntry>> iterator
					 = cache.Iterator();
				for (int i = 0; i < size; i++)
				{
					RpcCallCache.ClientRequest key = iterator.Next().Key;
					System.Console.Out.WriteLine("Entry " + key.GetClientId());
					Assert.Equal(Sharpen.Extensions.GetAddressByName("1.1.1." + (startEntry
						 + i)), key.GetClientId());
				}
				// Ensure cache entries are returned as in progress.
				for (int i_1 = 0; i_1 < size; i_1++)
				{
					RpcCallCache.CacheEntry e = cache.CheckOrAddToCache(Sharpen.Extensions.GetAddressByName
						("1.1.1." + (startEntry + i_1)), 0);
					NUnit.Framework.Assert.IsNotNull(e);
					Assert.True(e.IsInProgress());
					NUnit.Framework.Assert.IsFalse(e.IsCompleted());
				}
			}
		}
	}
}
