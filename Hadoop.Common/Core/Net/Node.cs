using Sharpen;

namespace Org.Apache.Hadoop.Net
{
	/// <summary>The interface defines a node in a network topology.</summary>
	/// <remarks>
	/// The interface defines a node in a network topology.
	/// A node may be a leave representing a data node or an inner
	/// node representing a datacenter or rack.
	/// Each data has a name and its location in the network is
	/// decided by a string with syntax similar to a file name.
	/// For example, a data node's name is hostname:port# and if it's located at
	/// rack "orange" in datacenter "dog", the string representation of its
	/// network location is /dog/orange
	/// </remarks>
	public interface Node
	{
		/// <returns>the string representation of this node's network location</returns>
		string GetNetworkLocation();

		/// <summary>Set this node's network location</summary>
		/// <param name="location">the location</param>
		void SetNetworkLocation(string location);

		/// <returns>this node's name</returns>
		string GetName();

		/// <returns>this node's parent</returns>
		Node GetParent();

		/// <summary>Set this node's parent</summary>
		/// <param name="parent">the parent</param>
		void SetParent(Node parent);

		/// <returns>
		/// this node's level in the tree.
		/// E.g. the root of a tree returns 0 and its children return 1
		/// </returns>
		int GetLevel();

		/// <summary>Set this node's level in the tree</summary>
		/// <param name="i">the level</param>
		void SetLevel(int i);
	}
}
