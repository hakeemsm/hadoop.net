using Sharpen;

namespace org.apache.hadoop.net
{
	/// <summary>A base class that implements interface Node</summary>
	public class NodeBase : org.apache.hadoop.net.Node
	{
		/// <summary>
		/// Path separator
		/// <value/>
		/// 
		/// </summary>
		public const char PATH_SEPARATOR = '/';

		/// <summary>
		/// Path separator as a string
		/// <value/>
		/// 
		/// </summary>
		public const string PATH_SEPARATOR_STR = "/";

		/// <summary>
		/// string representation of root
		/// <value/>
		/// 
		/// </summary>
		public const string ROOT = string.Empty;

		protected internal string name;

		protected internal string location;

		protected internal int level;

		protected internal org.apache.hadoop.net.Node parent;

		/// <summary>Default constructor</summary>
		public NodeBase()
		{
		}

		/// <summary>Construct a node from its path</summary>
		/// <param name="path">
		/// 
		/// a concatenation of this node's location, the path seperator, and its name
		/// </param>
		public NodeBase(string path)
		{
			//host:port#
			//string representation of this node's location
			//which level of the tree the node resides
			//its parent
			path = normalize(path);
			int index = path.LastIndexOf(PATH_SEPARATOR);
			if (index == -1)
			{
				set(ROOT, path);
			}
			else
			{
				set(Sharpen.Runtime.substring(path, index + 1), Sharpen.Runtime.substring(path, 0
					, index));
			}
		}

		/// <summary>Construct a node from its name and its location</summary>
		/// <param name="name">
		/// this node's name (can be null, must not contain
		/// <see cref="PATH_SEPARATOR"/>
		/// )
		/// </param>
		/// <param name="location">this node's location</param>
		public NodeBase(string name, string location)
		{
			set(name, normalize(location));
		}

		/// <summary>Construct a node from its name and its location</summary>
		/// <param name="name">
		/// this node's name (can be null, must not contain
		/// <see cref="PATH_SEPARATOR"/>
		/// )
		/// </param>
		/// <param name="location">this node's location</param>
		/// <param name="parent">this node's parent node</param>
		/// <param name="level">this node's level in the tree</param>
		public NodeBase(string name, string location, org.apache.hadoop.net.Node parent, 
			int level)
		{
			set(name, normalize(location));
			this.parent = parent;
			this.level = level;
		}

		/// <summary>set this node's name and location</summary>
		/// <param name="name">
		/// the (nullable) name -which cannot contain the
		/// <see cref="PATH_SEPARATOR"/>
		/// </param>
		/// <param name="location">the location</param>
		private void set(string name, string location)
		{
			if (name != null && name.contains(PATH_SEPARATOR_STR))
			{
				throw new System.ArgumentException("Network location name contains /: " + name);
			}
			this.name = (name == null) ? string.Empty : name;
			this.location = location;
		}

		/// <returns>this node's name</returns>
		public virtual string getName()
		{
			return name;
		}

		/// <returns>this node's network location</returns>
		public virtual string getNetworkLocation()
		{
			return location;
		}

		/// <summary>Set this node's network location</summary>
		/// <param name="location">the location</param>
		public virtual void setNetworkLocation(string location)
		{
			this.location = location;
		}

		/// <summary>Get the path of a node</summary>
		/// <param name="node">a non-null node</param>
		/// <returns>the path of a node</returns>
		public static string getPath(org.apache.hadoop.net.Node node)
		{
			return node.getNetworkLocation() + PATH_SEPARATOR_STR + node.getName();
		}

		/// <returns>this node's path as its string representation</returns>
		public override string ToString()
		{
			return getPath(this);
		}

		/// <summary>
		/// Normalize a path by stripping off any trailing
		/// <see cref="PATH_SEPARATOR"/>
		/// </summary>
		/// <param name="path">path to normalize.</param>
		/// <returns>
		/// the normalised path
		/// If <i>path</i>is null or empty
		/// <see cref="ROOT"/>
		/// is returned
		/// </returns>
		/// <exception cref="System.ArgumentException">
		/// if the first character of a non empty path
		/// is not
		/// <see cref="PATH_SEPARATOR"/>
		/// </exception>
		public static string normalize(string path)
		{
			if (path == null || path.Length == 0)
			{
				return ROOT;
			}
			if (path[0] != PATH_SEPARATOR)
			{
				throw new System.ArgumentException("Network Location path does not start with " +
					 PATH_SEPARATOR_STR + ": " + path);
			}
			int len = path.Length;
			if (path[len - 1] == PATH_SEPARATOR)
			{
				return Sharpen.Runtime.substring(path, 0, len - 1);
			}
			return path;
		}

		/// <returns>this node's parent</returns>
		public virtual org.apache.hadoop.net.Node getParent()
		{
			return parent;
		}

		/// <summary>Set this node's parent</summary>
		/// <param name="parent">the parent</param>
		public virtual void setParent(org.apache.hadoop.net.Node parent)
		{
			this.parent = parent;
		}

		/// <returns>
		/// this node's level in the tree.
		/// E.g. the root of a tree returns 0 and its children return 1
		/// </returns>
		public virtual int getLevel()
		{
			return level;
		}

		/// <summary>Set this node's level in the tree</summary>
		/// <param name="level">the level</param>
		public virtual void setLevel(int level)
		{
			this.level = level;
		}

		public static int locationToDepth(string location)
		{
			string normalizedLocation = normalize(location);
			int length = normalizedLocation.Length;
			int depth = 0;
			for (int i = 0; i < length; i++)
			{
				if (normalizedLocation[i] == PATH_SEPARATOR)
				{
					depth++;
				}
			}
			return depth;
		}
	}
}
