using System;


namespace Org.Apache.Hadoop.Net
{
	/// <summary>A base class that implements interface Node</summary>
	public class NodeBase : Node
	{
		/// <summary>
		/// Path separator
		/// <value/>
		/// 
		/// </summary>
		public const char PathSeparator = '/';

		/// <summary>
		/// Path separator as a string
		/// <value/>
		/// 
		/// </summary>
		public const string PathSeparatorStr = "/";

		/// <summary>
		/// string representation of root
		/// <value/>
		/// 
		/// </summary>
		public const string Root = string.Empty;

		protected internal string name;

		protected internal string location;

		protected internal int level;

		protected internal Node parent;

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
			path = Normalize(path);
			int index = path.LastIndexOf(PathSeparator);
			if (index == -1)
			{
				Set(Root, path);
			}
			else
			{
				Set(Runtime.Substring(path, index + 1), Runtime.Substring(path, 0
					, index));
			}
		}

		/// <summary>Construct a node from its name and its location</summary>
		/// <param name="name">
		/// this node's name (can be null, must not contain
		/// <see cref="PathSeparator"/>
		/// )
		/// </param>
		/// <param name="location">this node's location</param>
		public NodeBase(string name, string location)
		{
			Set(name, Normalize(location));
		}

		/// <summary>Construct a node from its name and its location</summary>
		/// <param name="name">
		/// this node's name (can be null, must not contain
		/// <see cref="PathSeparator"/>
		/// )
		/// </param>
		/// <param name="location">this node's location</param>
		/// <param name="parent">this node's parent node</param>
		/// <param name="level">this node's level in the tree</param>
		public NodeBase(string name, string location, Node parent, int level)
		{
			Set(name, Normalize(location));
			this.parent = parent;
			this.level = level;
		}

		/// <summary>set this node's name and location</summary>
		/// <param name="name">
		/// the (nullable) name -which cannot contain the
		/// <see cref="PathSeparator"/>
		/// </param>
		/// <param name="location">the location</param>
		private void Set(string name, string location)
		{
			if (name != null && name.Contains(PathSeparatorStr))
			{
				throw new ArgumentException("Network location name contains /: " + name);
			}
			this.name = (name == null) ? string.Empty : name;
			this.location = location;
		}

		/// <returns>this node's name</returns>
		public virtual string GetName()
		{
			return name;
		}

		/// <returns>this node's network location</returns>
		public virtual string GetNetworkLocation()
		{
			return location;
		}

		/// <summary>Set this node's network location</summary>
		/// <param name="location">the location</param>
		public virtual void SetNetworkLocation(string location)
		{
			this.location = location;
		}

		/// <summary>Get the path of a node</summary>
		/// <param name="node">a non-null node</param>
		/// <returns>the path of a node</returns>
		public static string GetPath(Node node)
		{
			return node.GetNetworkLocation() + PathSeparatorStr + node.GetName();
		}

		/// <returns>this node's path as its string representation</returns>
		public override string ToString()
		{
			return GetPath(this);
		}

		/// <summary>
		/// Normalize a path by stripping off any trailing
		/// <see cref="PathSeparator"/>
		/// </summary>
		/// <param name="path">path to normalize.</param>
		/// <returns>
		/// the normalised path
		/// If <i>path</i>is null or empty
		/// <see cref="Root"/>
		/// is returned
		/// </returns>
		/// <exception cref="System.ArgumentException">
		/// if the first character of a non empty path
		/// is not
		/// <see cref="PathSeparator"/>
		/// </exception>
		public static string Normalize(string path)
		{
			if (path == null || path.Length == 0)
			{
				return Root;
			}
			if (path[0] != PathSeparator)
			{
				throw new ArgumentException("Network Location path does not start with " + PathSeparatorStr
					 + ": " + path);
			}
			int len = path.Length;
			if (path[len - 1] == PathSeparator)
			{
				return Runtime.Substring(path, 0, len - 1);
			}
			return path;
		}

		/// <returns>this node's parent</returns>
		public virtual Node GetParent()
		{
			return parent;
		}

		/// <summary>Set this node's parent</summary>
		/// <param name="parent">the parent</param>
		public virtual void SetParent(Node parent)
		{
			this.parent = parent;
		}

		/// <returns>
		/// this node's level in the tree.
		/// E.g. the root of a tree returns 0 and its children return 1
		/// </returns>
		public virtual int GetLevel()
		{
			return level;
		}

		/// <summary>Set this node's level in the tree</summary>
		/// <param name="level">the level</param>
		public virtual void SetLevel(int level)
		{
			this.level = level;
		}

		public static int LocationToDepth(string location)
		{
			string normalizedLocation = Normalize(location);
			int length = normalizedLocation.Length;
			int depth = 0;
			for (int i = 0; i < length; i++)
			{
				if (normalizedLocation[i] == PathSeparator)
				{
					depth++;
				}
			}
			return depth;
		}
	}
}
