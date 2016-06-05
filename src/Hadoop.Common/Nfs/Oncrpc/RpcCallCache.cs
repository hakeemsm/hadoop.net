using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Annotations;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// This class is used for handling the duplicate <em>non-idempotenty</em> Rpc
	/// calls.
	/// </summary>
	/// <remarks>
	/// This class is used for handling the duplicate <em>non-idempotenty</em> Rpc
	/// calls. A non-idempotent request is processed as follows:
	/// <ul>
	/// <li>If the request is being processed for the first time, its state is
	/// in-progress in cache.</li>
	/// <li>If the request is retransimitted and is in-progress state, it is ignored.
	/// </li>
	/// <li>If the request is retransimitted and is completed, the previous response
	/// from the cache is sent back to the client.</li>
	/// </ul>
	/// <br />
	/// A request is identified by the client ID (address of the client) and
	/// transaction ID (xid) from the Rpc call.
	/// </remarks>
	public class RpcCallCache
	{
		public class CacheEntry
		{
			private RpcResponse response;

			public CacheEntry()
			{
				// null if no response has been sent
				response = null;
			}

			public virtual bool IsInProgress()
			{
				return response == null;
			}

			public virtual bool IsCompleted()
			{
				return response != null;
			}

			public virtual RpcResponse GetResponse()
			{
				return response;
			}

			public virtual void SetResponse(RpcResponse response)
			{
				this.response = response;
			}
		}

		/// <summary>
		/// Call that is used to track a client in the
		/// <see cref="RpcCallCache"/>
		/// </summary>
		public class ClientRequest
		{
			protected internal readonly IPAddress clientId;

			protected internal readonly int xid;

			public virtual IPAddress GetClientId()
			{
				return clientId;
			}

			public ClientRequest(IPAddress clientId, int xid)
			{
				this.clientId = clientId;
				this.xid = xid;
			}

			public override int GetHashCode()
			{
				return xid + clientId.GetHashCode() * 31;
			}

			public override bool Equals(object obj)
			{
				if (this == obj)
				{
					return true;
				}
				if (obj == null || !(obj is RpcCallCache.ClientRequest))
				{
					return false;
				}
				RpcCallCache.ClientRequest other = (RpcCallCache.ClientRequest)obj;
				return clientId.Equals(other.clientId) && (xid == other.xid);
			}
		}

		private readonly string program;

		private readonly IDictionary<RpcCallCache.ClientRequest, RpcCallCache.CacheEntry>
			 map;

		public RpcCallCache(string program, int maxEntries)
		{
			if (maxEntries <= 0)
			{
				throw new ArgumentException("Cache size is " + maxEntries + ". Should be > 0");
			}
			this.program = program;
			map = new _LinkedHashMap_114(this, maxEntries);
		}

		private sealed class _LinkedHashMap_114 : LinkedHashMap<RpcCallCache.ClientRequest
			, RpcCallCache.CacheEntry>
		{
			public _LinkedHashMap_114(RpcCallCache _enclosing, int maxEntries)
			{
				this._enclosing = _enclosing;
				this.maxEntries = maxEntries;
				this.serialVersionUID = 1L;
			}

			private const long serialVersionUID;

			protected override bool RemoveEldestEntry(KeyValuePair<RpcCallCache.ClientRequest
				, RpcCallCache.CacheEntry> eldest)
			{
				return this._enclosing.Size() > maxEntries;
			}

			private readonly RpcCallCache _enclosing;

			private readonly int maxEntries;
		}

		/// <summary>Return the program name</summary>
		public virtual string GetProgram()
		{
			return program;
		}

		/// <summary>Mark a request as completed and add corresponding response to the cache</summary>
		public virtual void CallCompleted(IPAddress clientId, int xid, RpcResponse response
			)
		{
			RpcCallCache.ClientRequest req = new RpcCallCache.ClientRequest(clientId, xid);
			RpcCallCache.CacheEntry e;
			lock (map)
			{
				e = map[req];
			}
			e.response = response;
		}

		/// <summary>Check the cache for an entry.</summary>
		/// <remarks>
		/// Check the cache for an entry. If it does not exist, add the request
		/// as in progress.
		/// </remarks>
		public virtual RpcCallCache.CacheEntry CheckOrAddToCache(IPAddress clientId, int 
			xid)
		{
			RpcCallCache.ClientRequest req = new RpcCallCache.ClientRequest(clientId, xid);
			RpcCallCache.CacheEntry e;
			lock (map)
			{
				e = map[req];
				if (e == null)
				{
					// Add an inprogress cache entry
					map[req] = new RpcCallCache.CacheEntry();
				}
			}
			return e;
		}

		/// <summary>Return number of cached entries</summary>
		public virtual int Size()
		{
			return map.Count;
		}

		/// <summary>Iterator to the cache entries</summary>
		/// <returns>iterator</returns>
		[VisibleForTesting]
		public virtual IEnumerator<KeyValuePair<RpcCallCache.ClientRequest, RpcCallCache.CacheEntry
			>> Iterator()
		{
			return map.GetEnumerator();
		}
	}
}
