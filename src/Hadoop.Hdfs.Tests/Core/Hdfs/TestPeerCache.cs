using System;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Net.Unix;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestPeerCache
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestPeerCache));

		private class FakePeer : Peer
		{
			private bool closed = false;

			private readonly bool hasDomain;

			private readonly DatanodeID dnId;

			public FakePeer(DatanodeID dnId, bool hasDomain)
			{
				this.dnId = dnId;
				this.hasDomain = hasDomain;
			}

			public virtual ReadableByteChannel GetInputStreamChannel()
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void SetReadTimeout(int timeoutMs)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int GetReceiveBufferSize()
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool GetTcpNoDelay()
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void SetWriteTimeout(int timeoutMs)
			{
				throw new NotSupportedException();
			}

			public virtual bool IsClosed()
			{
				return closed;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				closed = true;
			}

			public virtual string GetRemoteAddressString()
			{
				return dnId.GetInfoAddr();
			}

			public virtual string GetLocalAddressString()
			{
				return "127.0.0.1:123";
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual InputStream GetInputStream()
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual OutputStream GetOutputStream()
			{
				throw new NotSupportedException();
			}

			public virtual bool IsLocal()
			{
				return true;
			}

			public override string ToString()
			{
				return "FakePeer(dnId=" + dnId + ")";
			}

			public virtual DomainSocket GetDomainSocket()
			{
				if (!hasDomain)
				{
					return null;
				}
				// Return a mock which throws an exception whenever any function is
				// called.
				return Org.Mockito.Mockito.Mock<DomainSocket>(new _Answer_126());
			}

			private sealed class _Answer_126 : Answer<object>
			{
				public _Answer_126()
				{
				}

				/// <exception cref="System.Exception"/>
				public object Answer(InvocationOnMock invocation)
				{
					throw new RuntimeException("injected fault.");
				}
			}

			public override bool Equals(object o)
			{
				if (!(o is TestPeerCache.FakePeer))
				{
					return false;
				}
				TestPeerCache.FakePeer other = (TestPeerCache.FakePeer)o;
				return hasDomain == other.hasDomain && dnId.Equals(other.dnId);
			}

			public override int GetHashCode()
			{
				return dnId.GetHashCode() ^ (hasDomain ? 1 : 0);
			}

			public virtual bool HasSecureChannel()
			{
				return false;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAddAndRetrieve()
		{
			PeerCache cache = new PeerCache(3, 100000);
			DatanodeID dnId = new DatanodeID("192.168.0.1", "fakehostname", "fake_datanode_id"
				, 100, 101, 102, 103);
			TestPeerCache.FakePeer peer = new TestPeerCache.FakePeer(dnId, false);
			cache.Put(dnId, peer);
			NUnit.Framework.Assert.IsTrue(!peer.IsClosed());
			NUnit.Framework.Assert.AreEqual(1, cache.Size());
			NUnit.Framework.Assert.AreEqual(peer, cache.Get(dnId, false));
			NUnit.Framework.Assert.AreEqual(0, cache.Size());
			cache.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExpiry()
		{
			int Capacity = 3;
			int ExpiryPeriod = 10;
			PeerCache cache = new PeerCache(Capacity, ExpiryPeriod);
			DatanodeID[] dnIds = new DatanodeID[Capacity];
			TestPeerCache.FakePeer[] peers = new TestPeerCache.FakePeer[Capacity];
			for (int i = 0; i < Capacity; ++i)
			{
				dnIds[i] = new DatanodeID("192.168.0.1", "fakehostname_" + i, "fake_datanode_id", 
					100, 101, 102, 103);
				peers[i] = new TestPeerCache.FakePeer(dnIds[i], false);
			}
			for (int i_1 = 0; i_1 < Capacity; ++i_1)
			{
				cache.Put(dnIds[i_1], peers[i_1]);
			}
			// Wait for the peers to expire
			Sharpen.Thread.Sleep(ExpiryPeriod * 50);
			NUnit.Framework.Assert.AreEqual(0, cache.Size());
			// make sure that the peers were closed when they were expired
			for (int i_2 = 0; i_2 < Capacity; ++i_2)
			{
				NUnit.Framework.Assert.IsTrue(peers[i_2].IsClosed());
			}
			// sleep for another second and see if 
			// the daemon thread runs fine on empty cache
			Sharpen.Thread.Sleep(ExpiryPeriod * 50);
			cache.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEviction()
		{
			int Capacity = 3;
			PeerCache cache = new PeerCache(Capacity, 100000);
			DatanodeID[] dnIds = new DatanodeID[Capacity + 1];
			TestPeerCache.FakePeer[] peers = new TestPeerCache.FakePeer[Capacity + 1];
			for (int i = 0; i < dnIds.Length; ++i)
			{
				dnIds[i] = new DatanodeID("192.168.0.1", "fakehostname_" + i, "fake_datanode_id_"
					 + i, 100, 101, 102, 103);
				peers[i] = new TestPeerCache.FakePeer(dnIds[i], false);
			}
			for (int i_1 = 0; i_1 < Capacity; ++i_1)
			{
				cache.Put(dnIds[i_1], peers[i_1]);
			}
			// Check that the peers are cached
			NUnit.Framework.Assert.AreEqual(Capacity, cache.Size());
			// Add another entry and check that the first entry was evicted
			cache.Put(dnIds[Capacity], peers[Capacity]);
			NUnit.Framework.Assert.AreEqual(Capacity, cache.Size());
			NUnit.Framework.Assert.AreSame(null, cache.Get(dnIds[0], false));
			// Make sure that the other entries are still there
			for (int i_2 = 1; i_2 < Capacity; ++i_2)
			{
				Peer peer = cache.Get(dnIds[i_2], false);
				NUnit.Framework.Assert.AreSame(peers[i_2], peer);
				NUnit.Framework.Assert.IsTrue(!peer.IsClosed());
				peer.Close();
			}
			NUnit.Framework.Assert.AreEqual(1, cache.Size());
			cache.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultiplePeersWithSameKey()
		{
			int Capacity = 3;
			PeerCache cache = new PeerCache(Capacity, 100000);
			DatanodeID dnId = new DatanodeID("192.168.0.1", "fakehostname", "fake_datanode_id"
				, 100, 101, 102, 103);
			HashMultiset<TestPeerCache.FakePeer> peers = HashMultiset.Create(Capacity);
			for (int i = 0; i < Capacity; ++i)
			{
				TestPeerCache.FakePeer peer = new TestPeerCache.FakePeer(dnId, false);
				peers.AddItem(peer);
				cache.Put(dnId, peer);
			}
			// Check that all of the peers ended up in the cache
			NUnit.Framework.Assert.AreEqual(Capacity, cache.Size());
			while (!peers.IsEmpty())
			{
				Peer peer = cache.Get(dnId, false);
				NUnit.Framework.Assert.IsTrue(peer != null);
				NUnit.Framework.Assert.IsTrue(!peer.IsClosed());
				peers.Remove(peer);
			}
			NUnit.Framework.Assert.AreEqual(0, cache.Size());
			cache.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDomainSocketPeers()
		{
			int Capacity = 3;
			PeerCache cache = new PeerCache(Capacity, 100000);
			DatanodeID dnId = new DatanodeID("192.168.0.1", "fakehostname", "fake_datanode_id"
				, 100, 101, 102, 103);
			HashMultiset<TestPeerCache.FakePeer> peers = HashMultiset.Create(Capacity);
			for (int i = 0; i < Capacity; ++i)
			{
				TestPeerCache.FakePeer peer = new TestPeerCache.FakePeer(dnId, i == Capacity - 1);
				peers.AddItem(peer);
				cache.Put(dnId, peer);
			}
			// Check that all of the peers ended up in the cache
			NUnit.Framework.Assert.AreEqual(Capacity, cache.Size());
			// Test that get(requireDomainPeer=true) finds the peer with the 
			// domain socket.
			Peer peer_1 = cache.Get(dnId, true);
			NUnit.Framework.Assert.IsTrue(peer_1.GetDomainSocket() != null);
			peers.Remove(peer_1);
			// Test that get(requireDomainPeer=true) returns null when there are
			// no more peers with domain sockets.
			peer_1 = cache.Get(dnId, true);
			NUnit.Framework.Assert.IsTrue(peer_1 == null);
			// Check that all of the other peers ended up in the cache.
			while (!peers.IsEmpty())
			{
				peer_1 = cache.Get(dnId, false);
				NUnit.Framework.Assert.IsTrue(peer_1 != null);
				NUnit.Framework.Assert.IsTrue(!peer_1.IsClosed());
				peers.Remove(peer_1);
			}
			NUnit.Framework.Assert.AreEqual(0, cache.Size());
			cache.Close();
		}
	}
}
