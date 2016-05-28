using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Implementers of this class represent a NN resource whose availability can be
	/// checked.
	/// </summary>
	/// <remarks>
	/// Implementers of this class represent a NN resource whose availability can be
	/// checked. A resource can be either "required" or "redundant". All required
	/// resources must be available for the NN to continue operating. The NN will
	/// continue to operate as long as *any* redundant resource is available.
	/// </remarks>
	internal interface CheckableNameNodeResource
	{
		/// <summary>Is this resource currently available.</summary>
		/// <returns>true if and only if the resource in question is available.</returns>
		bool IsResourceAvailable();

		/// <summary>Is this resource required.</summary>
		/// <returns>true if and only if the resource in question is required for NN operation.
		/// 	</returns>
		bool IsRequired();
	}
}
