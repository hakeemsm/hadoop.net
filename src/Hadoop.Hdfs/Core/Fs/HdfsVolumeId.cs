using Com.Google.Common.Base;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// HDFS-specific volume identifier which implements
	/// <see cref="VolumeId"/>
	/// . Can be
	/// used to differentiate between the data directories on a single datanode. This
	/// identifier is only unique on a per-datanode basis.
	/// </summary>
	public class HdfsVolumeId : VolumeId
	{
		private readonly byte[] id;

		public HdfsVolumeId(byte[] id)
		{
			Preconditions.CheckNotNull(id, "id cannot be null");
			this.id = id;
		}

		public virtual int CompareTo(VolumeId arg0)
		{
			if (arg0 == null)
			{
				return 1;
			}
			return GetHashCode() - arg0.GetHashCode();
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder().Append(id).ToHashCode();
		}

		public override bool Equals(object obj)
		{
			if (obj == null || obj.GetType() != GetType())
			{
				return false;
			}
			if (obj == this)
			{
				return true;
			}
			Org.Apache.Hadoop.FS.HdfsVolumeId that = (Org.Apache.Hadoop.FS.HdfsVolumeId)obj;
			return new EqualsBuilder().Append(this.id, that.id).IsEquals();
		}

		public override string ToString()
		{
			return StringUtils.ByteToHexString(id);
		}
	}
}
