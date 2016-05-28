using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task.Reduce
{
	public abstract class MapOutput<K, V>
	{
		private static AtomicInteger Id = new AtomicInteger(0);

		private readonly int id;

		private readonly TaskAttemptID mapId;

		private readonly long size;

		private readonly bool primaryMapOutput;

		public MapOutput(TaskAttemptID mapId, long size, bool primaryMapOutput)
		{
			this.id = Id.IncrementAndGet();
			this.mapId = mapId;
			this.size = size;
			this.primaryMapOutput = primaryMapOutput;
		}

		public virtual bool IsPrimaryMapOutput()
		{
			return primaryMapOutput;
		}

		public override bool Equals(object obj)
		{
			if (obj is Org.Apache.Hadoop.Mapreduce.Task.Reduce.MapOutput)
			{
				return id == ((Org.Apache.Hadoop.Mapreduce.Task.Reduce.MapOutput)obj).id;
			}
			return false;
		}

		public override int GetHashCode()
		{
			return id;
		}

		public virtual TaskAttemptID GetMapId()
		{
			return mapId;
		}

		public virtual long GetSize()
		{
			return size;
		}

		/// <exception cref="System.IO.IOException"/>
		public abstract void Shuffle(MapHost host, InputStream input, long compressedLength
			, long decompressedLength, ShuffleClientMetrics metrics, Reporter reporter);

		/// <exception cref="System.IO.IOException"/>
		public abstract void Commit();

		public abstract void Abort();

		public abstract string GetDescription();

		public override string ToString()
		{
			return "MapOutput(" + mapId + ", " + GetDescription() + ")";
		}

		public class MapOutputComparator<K, V> : IComparer<MapOutput<K, V>>
		{
			public virtual int Compare(MapOutput<K, V> o1, MapOutput<K, V> o2)
			{
				if (o1.id == o2.id)
				{
					return 0;
				}
				if (o1.size < o2.size)
				{
					return -1;
				}
				else
				{
					if (o1.size > o2.size)
					{
						return 1;
					}
				}
				if (o1.id < o2.id)
				{
					return -1;
				}
				else
				{
					return 1;
				}
			}
		}
	}
}
