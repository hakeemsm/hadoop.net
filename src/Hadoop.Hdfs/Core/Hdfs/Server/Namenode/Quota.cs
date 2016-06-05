using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Quota types.</summary>
	[System.Serializable]
	public sealed class Quota
	{
		/// <summary>The namespace usage, i.e.</summary>
		/// <remarks>The namespace usage, i.e. the number of name objects.</remarks>
		public static readonly Quota Namespace = new Quota();

		/// <summary>The storage space usage in bytes including replication.</summary>
		public static readonly Quota Storagespace = new Quota();

		/// <summary>Counters for quota counts.</summary>
		public class Counts : EnumCounters<Quota>
		{
			/// <returns>a new counter with the given namespace and storagespace usages.</returns>
			public static Quota.Counts NewInstance(long @namespace, long storagespace)
			{
				Quota.Counts c = new Quota.Counts();
				c.Set(Quota.Namespace, @namespace);
				c.Set(Quota.Storagespace, storagespace);
				return c;
			}

			public static Quota.Counts NewInstance()
			{
				return NewInstance(0, 0);
			}

			internal Counts()
				: base(typeof(Quota))
			{
			}
		}

		/// <summary>
		/// Is quota violated?
		/// The quota is violated if quota is set and usage &gt; quota.
		/// </summary>
		internal static bool IsViolated(long quota, long usage)
		{
			return quota >= 0 && usage > quota;
		}

		/// <summary>
		/// Is quota violated?
		/// The quota is violated if quota is set, delta &gt; 0 and usage + delta &gt; quota.
		/// </summary>
		internal static bool IsViolated(long quota, long usage, long delta)
		{
			return quota >= 0 && delta > 0 && usage > quota - delta;
		}
	}
}
