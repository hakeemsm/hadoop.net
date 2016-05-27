using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Shell;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Shell.Find
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
		private TextWriter @out;

		/// <summary>Error stream to be used.</summary>
		private TextWriter err;

		/// <summary>Input stream to be used.</summary>
		private InputStream @in;

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
		private long startTime = new DateTime().GetTime();

		/// <summary>Depth at which to start applying expressions.</summary>
		private int minDepth = 0;

		/// <summary>Depth at which to stop applying expressions.</summary>
		private int maxDepth = int.MaxValue;

		/// <summary>Factory for retrieving command classes.</summary>
		private CommandFactory commandFactory;

		/// <summary>Configuration object.</summary>
		private Configuration configuration = new Configuration();

		/// <summary>Sets the output stream to be used.</summary>
		/// <param name="out">output stream to be used</param>
		public virtual void SetOut(TextWriter @out)
		{
			this.@out = @out;
		}

		/// <summary>Returns the output stream to be used.</summary>
		/// <returns>output stream to be used</returns>
		public virtual TextWriter GetOut()
		{
			return this.@out;
		}

		/// <summary>Sets the error stream to be used.</summary>
		/// <param name="err">error stream to be used</param>
		public virtual void SetErr(TextWriter err)
		{
			this.err = err;
		}

		/// <summary>Returns the error stream to be used.</summary>
		/// <returns>error stream to be used</returns>
		public virtual TextWriter GetErr()
		{
			return this.err;
		}

		/// <summary>Sets the input stream to be used.</summary>
		/// <param name="in">input stream to be used</param>
		public virtual void SetIn(InputStream @in)
		{
			this.@in = @in;
		}

		/// <summary>Returns the input stream to be used.</summary>
		/// <returns>input stream to be used</returns>
		public virtual InputStream GetIn()
		{
			return this.@in;
		}

		/// <summary>
		/// Sets flag indicating whether the expression should be applied to the
		/// directory tree depth first.
		/// </summary>
		/// <param name="depthFirst">true indicates depth first traversal</param>
		public virtual void SetDepthFirst(bool depthFirst)
		{
			this.depthFirst = depthFirst;
		}

		/// <summary>Should directory tree be traversed depth first?</summary>
		/// <returns>true indicate depth first traversal</returns>
		public virtual bool IsDepthFirst()
		{
			return this.depthFirst;
		}

		/// <summary>Sets flag indicating whether symbolic links should be followed.</summary>
		/// <param name="followLink">true indicates follow links</param>
		public virtual void SetFollowLink(bool followLink)
		{
			this.followLink = followLink;
		}

		/// <summary>Should symbolic links be follows?</summary>
		/// <returns>true indicates links should be followed</returns>
		public virtual bool IsFollowLink()
		{
			return this.followLink;
		}

		/// <summary>
		/// Sets flag indicating whether command line symbolic links should be
		/// followed.
		/// </summary>
		/// <param name="followArgLink">true indicates follow links</param>
		public virtual void SetFollowArgLink(bool followArgLink)
		{
			this.followArgLink = followArgLink;
		}

		/// <summary>Should command line symbolic links be follows?</summary>
		/// <returns>true indicates links should be followed</returns>
		public virtual bool IsFollowArgLink()
		{
			return this.followArgLink;
		}

		/// <summary>
		/// Returns the start time of this
		/// <see cref="Find"/>
		/// command.
		/// </summary>
		/// <returns>start time (in milliseconds since epoch)</returns>
		public virtual long GetStartTime()
		{
			return this.startTime;
		}

		/// <summary>
		/// Set the start time of this
		/// <see cref="Find"/>
		/// command.
		/// </summary>
		/// <param name="time">start time (in milliseconds since epoch)</param>
		public virtual void SetStartTime(long time)
		{
			this.startTime = time;
		}

		/// <summary>Returns the minimum depth for applying expressions.</summary>
		/// <returns>min depth</returns>
		public virtual int GetMinDepth()
		{
			return this.minDepth;
		}

		/// <summary>Sets the minimum depth for applying expressions.</summary>
		/// <param name="minDepth">minimum depth</param>
		public virtual void SetMinDepth(int minDepth)
		{
			this.minDepth = minDepth;
		}

		/// <summary>Returns the maximum depth for applying expressions.</summary>
		/// <returns>maximum depth</returns>
		public virtual int GetMaxDepth()
		{
			return this.maxDepth;
		}

		/// <summary>Sets the maximum depth for applying expressions.</summary>
		/// <param name="maxDepth">maximum depth</param>
		public virtual void SetMaxDepth(int maxDepth)
		{
			this.maxDepth = maxDepth;
		}

		/// <summary>Set the command factory.</summary>
		/// <param name="factory">
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Shell.CommandFactory"/>
		/// </param>
		public virtual void SetCommandFactory(CommandFactory factory)
		{
			this.commandFactory = factory;
		}

		/// <summary>Return the command factory.</summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.FS.Shell.CommandFactory"/>
		/// </returns>
		public virtual CommandFactory GetCommandFactory()
		{
			return this.commandFactory;
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// </summary>
		/// <param name="configuration">
		/// 
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// </param>
		public virtual void SetConfiguration(Configuration configuration)
		{
			this.configuration = configuration;
		}

		/// <summary>
		/// Return the
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// return configuration
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// </summary>
		public virtual Configuration GetConfiguration()
		{
			return this.configuration;
		}
	}
}
