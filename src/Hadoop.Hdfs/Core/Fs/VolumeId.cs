using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Opaque interface that identifies a disk location.</summary>
	/// <remarks>
	/// Opaque interface that identifies a disk location. Subclasses
	/// should implement
	/// <see cref="System.IComparable{T}"/>
	/// and override both equals and hashCode.
	/// </remarks>
	public interface VolumeId : Comparable<VolumeId>
	{
		int CompareTo(VolumeId arg0);

		int GetHashCode();

		bool Equals(object obj);
	}
}
