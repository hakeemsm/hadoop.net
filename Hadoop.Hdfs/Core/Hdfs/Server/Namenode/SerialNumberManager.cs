using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Manage name-to-serial-number maps for users and groups.</summary>
	internal class SerialNumberManager
	{
		/// <summary>
		/// This is the only instance of
		/// <see cref="SerialNumberManager"/>
		/// .
		/// </summary>
		internal static readonly Org.Apache.Hadoop.Hdfs.Server.Namenode.SerialNumberManager
			 Instance = new Org.Apache.Hadoop.Hdfs.Server.Namenode.SerialNumberManager();

		private readonly SerialNumberManager.SerialNumberMap<string> usermap = new SerialNumberManager.SerialNumberMap
			<string>();

		private readonly SerialNumberManager.SerialNumberMap<string> groupmap = new SerialNumberManager.SerialNumberMap
			<string>();

		private SerialNumberManager()
		{
			{
				GetUserSerialNumber(null);
				GetGroupSerialNumber(null);
			}
		}

		internal virtual int GetUserSerialNumber(string u)
		{
			return usermap.Get(u);
		}

		internal virtual int GetGroupSerialNumber(string g)
		{
			return groupmap.Get(g);
		}

		internal virtual string GetUser(int n)
		{
			return usermap.Get(n);
		}

		internal virtual string GetGroup(int n)
		{
			return groupmap.Get(n);
		}

		private class SerialNumberMap<T>
		{
			private readonly AtomicInteger max = new AtomicInteger(1);

			private readonly ConcurrentMap<T, int> t2i = new ConcurrentHashMap<T, int>();

			private readonly ConcurrentMap<int, T> i2t = new ConcurrentHashMap<int, T>();

			internal virtual int Get(T t)
			{
				if (t == null)
				{
					return 0;
				}
				int sn = t2i[t];
				if (sn == null)
				{
					sn = max.GetAndIncrement();
					int old = t2i.PutIfAbsent(t, sn);
					if (old != null)
					{
						return old;
					}
					i2t[sn] = t;
				}
				return sn;
			}

			internal virtual T Get(int i)
			{
				if (i == 0)
				{
					return null;
				}
				T t = i2t[i];
				if (t == null)
				{
					throw new InvalidOperationException("!i2t.containsKey(" + i + "), this=" + this);
				}
				return t;
			}

			public override string ToString()
			{
				return "max=" + max + ",\n  t2i=" + t2i + ",\n  i2t=" + i2t;
			}
		}
	}
}
