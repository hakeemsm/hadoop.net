using Sharpen;

namespace org.apache.hadoop.fs.shell.find
{
	/// <summary>
	/// Options to be used by the
	/// <see cref="Find"/>
	/// command and its
	/// <see cref="Expression"/>
	/// s.
	/// </summary>
	public class FindOptions
	{
		/// <summary>Output stream to be used.</summary>
		private System.IO.TextWriter @out;

		/// <summary>Error stream to be used.</summary>
		private System.IO.TextWriter err;

		/// <summary>Input stream to be used.</summary>
		private java.io.InputStream @in;

		/// <summary>
		/// Indicates whether the expression should be applied to the directory tree
		/// depth first.
		/// </summary>
		private bool depthFirst = false;

		/// <summary>Indicates whether symbolic links should be followed.</summary>
		private bool followLink = false;

		/// <summary>
		/// Indicates whether symbolic links specified as command arguments should be
		/// followed.
		/// </summary>
		private bool followArgLink = false;

		/// <summary>Start time of the find process.</summary>
		private long startTime = new System.DateTime().getTime();

		/// <summary>Depth at which to start applying expressions.</summary>
		private int minDepth = 0;

		/// <summary>Depth at which to stop applying expressions.</summary>
		private int maxDepth = int.MaxValue;

		/// <summary>Factory for retrieving command classes.</summary>
		private org.apache.hadoop.fs.shell.CommandFactory commandFactory;

		/// <summary>Configuration object.</summary>
		private org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration
			();

		/// <summary>Sets the output stream to be used.</summary>
		/// <param name="out">output stream to be used</param>
		public virtual void setOut(System.IO.TextWriter @out)
		{
			this.@out = @out;
		}

		/// <summary>Returns the output stream to be used.</summary>
		/// <returns>output stream to be used</returns>
		public virtual System.IO.TextWriter getOut()
		{
			return this.@out;
		}

		/// <summary>Sets the error stream to be used.</summary>
		/// <param name="err">error stream to be used</param>
		public virtual void setErr(System.IO.TextWriter err)
		{
			this.err = err;
		}

		/// <summary>Returns the error stream to be used.</summary>
		/// <returns>error stream to be used</returns>
		public virtual System.IO.TextWriter getErr()
		{
			return this.err;
		}

		/// <summary>Sets the input stream to be used.</summary>
		/// <param name="in">input stream to be used</param>
		public virtual void setIn(java.io.InputStream @in)
		{
			this.@in = @in;
		}

		/// <summary>Returns the input stream to be used.</summary>
		/// <returns>input stream to be used</returns>
		public virtual java.io.InputStream getIn()
		{
			return this.@in;
		}

		/// <summary>
		/// Sets flag indicating whether the expression should be applied to the
		/// directory tree depth first.
		/// </summary>
		/// <param name="depthFirst">true indicates depth first traversal</param>
		public virtual void setDepthFirst(bool depthFirst)
		{
			this.depthFirst = depthFirst;
		}

		/// <summary>Should directory tree be traversed depth first?</summary>
		/// <returns>true indicate depth first traversal</returns>
		public virtual bool isDepthFirst()
		{
			return this.depthFirst;
		}

		/// <summary>Sets flag indicating whether symbolic links should be followed.</summary>
		/// <param name="followLink">true indicates follow links</param>
		public virtual void setFollowLink(bool followLink)
		{
			this.followLink = followLink;
		}

		/// <summary>Should symbolic links be follows?</summary>
		/// <returns>true indicates links should be followed</returns>
		public virtual bool isFollowLink()
		{
			return this.followLink;
		}

		/// <summary>
		/// Sets flag indicating whether command line symbolic links should be
		/// followed.
		/// </summary>
		/// <param name="followArgLink">true indicates follow links</param>
		public virtual void setFollowArgLink(bool followArgLink)
		{
			this.followArgLink = followArgLink;
		}

		/// <summary>Should command line symbolic links be follows?</summary>
		/// <returns>true indicates links should be followed</returns>
		public virtual bool isFollowArgLink()
		{
			return this.followArgLink;
		}

		/// <summary>
		/// Returns the start time of this
		/// <see cref="Find"/>
		/// command.
		/// </summary>
		/// <returns>start time (in milliseconds since epoch)</returns>
		public virtual long getStartTime()
		{
			return this.startTime;
		}

		/// <summary>
		/// Set the start time of this
		/// <see cref="Find"/>
		/// command.
		/// </summary>
		/// <param name="time">start time (in milliseconds since epoch)</param>
		public virtual void setStartTime(long time)
		{
			this.startTime = time;
		}

		/// <summary>Returns the minimum depth for applying expressions.</summary>
		/// <returns>min depth</returns>
		public virtual int getMinDepth()
		{
			return this.minDepth;
		}

		/// <summary>Sets the minimum depth for applying expressions.</summary>
		/// <param name="minDepth">minimum depth</param>
		public virtual void setMinDepth(int minDepth)
		{
			this.minDepth = minDepth;
		}

		/// <summary>Returns the maximum depth for applying expressions.</summary>
		/// <returns>maximum depth</returns>
		public virtual int getMaxDepth()
		{
			return this.maxDepth;
		}

		/// <summary>Sets the maximum depth for applying expressions.</summary>
		/// <param name="maxDepth">maximum depth</param>
		public virtual void setMaxDepth(int maxDepth)
		{
			this.maxDepth = maxDepth;
		}

		/// <summary>Set the command factory.</summary>
		/// <param name="factory">
		/// 
		/// <see cref="org.apache.hadoop.fs.shell.CommandFactory"/>
		/// </param>
		public virtual void setCommandFactory(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			this.commandFactory = factory;
		}

		/// <summary>Return the command factory.</summary>
		/// <returns>
		/// 
		/// <see cref="org.apache.hadoop.fs.shell.CommandFactory"/>
		/// </returns>
		public virtual org.apache.hadoop.fs.shell.CommandFactory getCommandFactory()
		{
			return this.commandFactory;
		}

		/// <summary>
		/// Set the
		/// <see cref="org.apache.hadoop.conf.Configuration"/>
		/// </summary>
		/// <param name="configuration">
		/// 
		/// <see cref="org.apache.hadoop.conf.Configuration"/>
		/// </param>
		public virtual void setConfiguration(org.apache.hadoop.conf.Configuration configuration
			)
		{
			this.configuration = configuration;
		}

		/// <summary>
		/// Return the
		/// <see cref="org.apache.hadoop.conf.Configuration"/>
		/// return configuration
		/// <see cref="org.apache.hadoop.conf.Configuration"/>
		/// </summary>
		public virtual org.apache.hadoop.conf.Configuration getConfiguration()
		{
			return this.configuration;
		}
	}
}
