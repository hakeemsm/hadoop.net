using System.Collections.Generic;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>This class manages the include and exclude files for HDFS.</summary>
	/// <remarks>
	/// This class manages the include and exclude files for HDFS.
	/// <p/>
	/// These files control which DataNodes the NameNode expects to see in the
	/// cluster.  Loosely speaking, the include file, if it exists and is not
	/// empty, is a list of everything we expect to see.  The exclude file is
	/// a list of everything we want to ignore if we do see it.
	/// <p/>
	/// Entries may or may not specify a port.  If they don't, we consider
	/// them to apply to every DataNode on that host. The code canonicalizes the
	/// entries into IP addresses.
	/// <p/>
	/// <p/>
	/// The code ignores all entries that the DNS fails to resolve their IP
	/// addresses. This is okay because by default the NN rejects the registrations
	/// of DNs when it fails to do a forward and reverse lookup. Note that DNS
	/// resolutions are only done during the loading time to minimize the latency.
	/// </remarks>
	internal class HostFileManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(HostFileManager));

		private HostFileManager.HostSet includes = new HostFileManager.HostSet();

		private HostFileManager.HostSet excludes = new HostFileManager.HostSet();

		/// <exception cref="System.IO.IOException"/>
		private static HostFileManager.HostSet ReadFile(string type, string filename)
		{
			HostFileManager.HostSet res = new HostFileManager.HostSet();
			if (!filename.IsEmpty())
			{
				HashSet<string> entrySet = new HashSet<string>();
				HostsFileReader.ReadFileToSet(type, filename, entrySet);
				foreach (string str in entrySet)
				{
					IPEndPoint addr = ParseEntry(type, filename, str);
					if (addr != null)
					{
						res.Add(addr);
					}
				}
			}
			return res;
		}

		[VisibleForTesting]
		internal static IPEndPoint ParseEntry(string type, string fn, string line)
		{
			try
			{
				URI uri = new URI("dummy", line, null, null, null);
				int port = uri.GetPort() == -1 ? 0 : uri.GetPort();
				IPEndPoint addr = new IPEndPoint(uri.GetHost(), port);
				if (addr.IsUnresolved())
				{
					Log.Warn(string.Format("Failed to resolve address `%s` in `%s`. " + "Ignoring in the %s list."
						, line, fn, type));
					return null;
				}
				return addr;
			}
			catch (URISyntaxException)
			{
				Log.Warn(string.Format("Failed to parse `%s` in `%s`. " + "Ignoring in " + "the %s list."
					, line, fn, type));
			}
			return null;
		}

		internal static IPEndPoint ResolvedAddressFromDatanodeID(DatanodeID id)
		{
			return new IPEndPoint(id.GetIpAddr(), id.GetXferPort());
		}

		internal virtual HostFileManager.HostSet GetIncludes()
		{
			lock (this)
			{
				return includes;
			}
		}

		internal virtual HostFileManager.HostSet GetExcludes()
		{
			lock (this)
			{
				return excludes;
			}
		}

		// If the includes list is empty, act as if everything is in the
		// includes list.
		internal virtual bool IsIncluded(DatanodeID dn)
		{
			lock (this)
			{
				return includes.IsEmpty() || includes.Match(ResolvedAddressFromDatanodeID(dn));
			}
		}

		internal virtual bool IsExcluded(DatanodeID dn)
		{
			lock (this)
			{
				return excludes.Match(ResolvedAddressFromDatanodeID(dn));
			}
		}

		internal virtual bool HasIncludes()
		{
			lock (this)
			{
				return !includes.IsEmpty();
			}
		}

		/// <summary>Read the includes and excludes lists from the named files.</summary>
		/// <remarks>
		/// Read the includes and excludes lists from the named files.  Any previous
		/// includes and excludes lists are discarded.
		/// </remarks>
		/// <param name="includeFile">the path to the new includes list</param>
		/// <param name="excludeFile">the path to the new excludes list</param>
		/// <exception cref="System.IO.IOException">thrown if there is a problem reading one of the files
		/// 	</exception>
		internal virtual void Refresh(string includeFile, string excludeFile)
		{
			HostFileManager.HostSet newIncludes = ReadFile("included", includeFile);
			HostFileManager.HostSet newExcludes = ReadFile("excluded", excludeFile);
			Refresh(newIncludes, newExcludes);
		}

		/// <summary>Set the includes and excludes lists by the new HostSet instances.</summary>
		/// <remarks>
		/// Set the includes and excludes lists by the new HostSet instances. The
		/// old instances are discarded.
		/// </remarks>
		/// <param name="newIncludes">the new includes list</param>
		/// <param name="newExcludes">the new excludes list</param>
		[VisibleForTesting]
		internal virtual void Refresh(HostFileManager.HostSet newIncludes, HostFileManager.HostSet
			 newExcludes)
		{
			lock (this)
			{
				includes = newIncludes;
				excludes = newExcludes;
			}
		}

		/// <summary>The HostSet allows efficient queries on matching wildcard addresses.</summary>
		/// <remarks>
		/// The HostSet allows efficient queries on matching wildcard addresses.
		/// <p/>
		/// For InetSocketAddress A and B with the same host address,
		/// we define a partial order between A and B, A &lt;= B iff A.getPort() == B
		/// .getPort() || B.getPort() == 0.
		/// </remarks>
		internal class HostSet : IEnumerable<IPEndPoint>
		{
			private readonly Multimap<IPAddress, int> addrs = HashMultimap.Create();

			// Host -> lists of ports
			/// <summary>
			/// The function that checks whether there exists an entry foo in the set
			/// so that foo &lt;= addr.
			/// </summary>
			internal virtual bool MatchedBy(IPEndPoint addr)
			{
				ICollection<int> ports = addrs.Get(addr.Address);
				return addr.Port == 0 ? !ports.IsEmpty() : ports.Contains(addr.Port);
			}

			/// <summary>
			/// The function that checks whether there exists an entry foo in the set
			/// so that addr &lt;= foo.
			/// </summary>
			internal virtual bool Match(IPEndPoint addr)
			{
				int port = addr.Port;
				ICollection<int> ports = addrs.Get(addr.Address);
				bool exactMatch = ports.Contains(port);
				bool genericMatch = ports.Contains(0);
				return exactMatch || genericMatch;
			}

			internal virtual bool IsEmpty()
			{
				return addrs.IsEmpty();
			}

			internal virtual int Size()
			{
				return addrs.Size();
			}

			internal virtual void Add(IPEndPoint addr)
			{
				Preconditions.CheckArgument(!addr.IsUnresolved());
				addrs.Put(addr.Address, addr.Port);
			}

			public override IEnumerator<IPEndPoint> GetEnumerator()
			{
				return new _UnmodifiableIterator_205(this);
			}

			private sealed class _UnmodifiableIterator_205 : UnmodifiableIterator<IPEndPoint>
			{
				public _UnmodifiableIterator_205()
				{
					this.it = this._enclosing.addrs.Entries().GetEnumerator();
				}

				private readonly IEnumerator<KeyValuePair<IPAddress, int>> it;

				public override bool HasNext()
				{
					return this.it.HasNext();
				}

				public override IPEndPoint Next()
				{
					KeyValuePair<IPAddress, int> e = this.it.Next();
					return new IPEndPoint(e.Key, e.Value);
				}
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder("HostSet(");
				Joiner.On(",").AppendTo(sb, Iterators.Transform(GetEnumerator(), new _Function_226
					()));
				return sb.Append(")").ToString();
			}

			private sealed class _Function_226 : Function<IPEndPoint, string>
			{
				public _Function_226()
				{
				}

				public string Apply(IPEndPoint addr)
				{
					System.Diagnostics.Debug.Assert(addr != null);
					return addr.Address.GetHostAddress() + ":" + addr.Port;
				}
			}
		}
	}
}
