using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>The content types such as file, directory and symlink to be computed.</summary>
	[System.Serializable]
	public sealed class Content
	{
		/// <summary>The number of files.</summary>
		public static readonly Content File = new Content();

		/// <summary>The number of directories.</summary>
		public static readonly Content Directory = new Content();

		/// <summary>The number of symlinks.</summary>
		public static readonly Content Symlink = new Content();

		/// <summary>The total of file length in bytes.</summary>
		public static readonly Content Length = new Content();

		/// <summary>The total of disk space usage in bytes including replication.</summary>
		public static readonly Content Diskspace = new Content();

		/// <summary>The number of snapshots.</summary>
		public static readonly Content Snapshot = new Content();

		/// <summary>The number of snapshottable directories.</summary>
		public static readonly Content SnapshottableDirectory = new Content();

		/// <summary>Content counts.</summary>
		public class Counts : EnumCounters<Content>
		{
			public static Content.Counts NewInstance()
			{
				return new Content.Counts();
			}

			private Counts()
				: base(typeof(Content))
			{
			}
		}

		private sealed class _Factory_55 : EnumCounters.Factory<Content, Content.Counts>
		{
			public _Factory_55()
			{
			}

			public Content.Counts NewInstance()
			{
				return Content.Counts.NewInstance();
			}
		}

		private static readonly EnumCounters.Factory<Content, Content.Counts> Factory = new 
			_Factory_55();

		/// <summary>A map of counters for the current state and the snapshots.</summary>
		public class CountsMap : EnumCounters.Map<Content.CountsMap.Key, Content, Content.Counts
			>
		{
			/// <summary>The key type of the map.</summary>
			public enum Key
			{
				Current,
				Snapshot
			}

			internal CountsMap()
				: base(Content.Factory)
			{
			}
		}
	}
}
